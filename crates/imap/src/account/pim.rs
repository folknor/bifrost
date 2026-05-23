use std::collections::HashMap;
use std::time::SystemTime;

use bifrost_types::compose::{Address, AttachmentHandle, DraftHandle, DraftPatch, IdentityId};
use bifrost_types::container::{
    Container, ContainerId, ContainerKind, FolderRole, MutationTarget, Provenance,
};
use bifrost_types::hydration::{HydrationProjection, Message, ThreadHydration};
use bifrost_types::ids::{ObjectId, ThreadId};
use bifrost_types::page::Page;
use bifrost_types::search::{SearchFilter, SearchRequest};
use bifrost_types::settings::{Identity, IdentityPatch, QuotaInfo, VacationConfig};
use bifrost_types::{AccountFuture, Error as AccountError, LabelId, ProtocolKind};
use chrono::{DateTime, Datelike, Utc};

use crate::types::{
    FetchAttr, Flag, MailboxAttribute, MailboxName, SearchCriteria, StatusItem, StoreOperation,
    ThreadNode,
};

use super::{
    DecodedObjectId, ImapAccount, account_error, decode_object_id, decode_thread_id,
    encode_object_id, encode_thread_id, factory, uid_set_from_u32,
};

pub(crate) fn add_to_container(
    account: ImapAccount,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let destination = folder_from_container(&container)?;
        let ids = decoded_targets(&target)?;
        copy_messages(&account, ids, &destination).await
    })
}

pub(crate) fn remove_from_container(
    account: ImapAccount,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let source = folder_from_container(&container)?;
        let ids = decoded_targets(&target)?
            .into_iter()
            .filter(|id| id.folder == source)
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Err(AccountError::Unsupported);
        }
        delete_messages(&account, ids).await
    })
}

pub(crate) fn set_keyword(
    account: ImapAccount,
    target: MutationTarget,
    keyword: String,
    value: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let flag = imap_flag_for_keyword(&keyword);
        let ids = decoded_targets(&target)?;
        set_flag(&account, ids, flag, value).await
    })
}

pub(crate) fn set_is_read(
    account: ImapAccount,
    target: MutationTarget,
    is_read: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let ids = decoded_targets(&target)?;
        set_flag(&account, ids, Flag::Seen, is_read).await
    })
}

pub(crate) fn unsupported_unit() -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async { Err(AccountError::Unsupported) })
}

pub(crate) fn unsupported_object() -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async { Err(AccountError::Unsupported) })
}

pub(crate) fn unsupported_attachment() -> AccountFuture<Result<AttachmentHandle, AccountError>> {
    Box::pin(async { Err(AccountError::Unsupported) })
}

pub(crate) fn draft_create(
    account: ImapAccount,
    patch: DraftPatch,
) -> AccountFuture<Result<DraftHandle, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.draft_create {
            return Err(AccountError::Unsupported);
        }
        let folder = role_folder(&account, FolderRole::Drafts).ok_or(AccountError::Unsupported)?;
        let raw = draft_patch_to_rfc5322(&patch)?;
        let conn = account.pool.dial_idle().await.map_err(account_error)?;
        let appended = conn
            .append(
                folder.as_str(),
                &[Flag::Draft],
                None,
                &raw,
                account.command_timeout(),
            )
            .await
            .map_err(account_error)?;
        let Some((uidvalidity, uid)) = appended else {
            return Err(AccountError::Unsupported);
        };
        Ok(DraftHandle(encode_object_id(&folder, uidvalidity, uid).0))
    })
}

pub(crate) fn draft_discard(
    account: ImapAccount,
    draft: DraftHandle,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let id = decode_object_id(&ObjectId(draft.0))?;
        delete_messages(&account, vec![id]).await
    })
}

pub(crate) fn search(
    account: ImapAccount,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.search {
            return Err(AccountError::Unsupported);
        }
        let plan = search_plan(&request)?;
        let mut threads = Vec::new();
        for folder in search_folders(&account, plan.folder.as_ref()) {
            let mut conn = account
                .checkout_for_folder(&folder)
                .await
                .map_err(account_error)?;
            let selected = account
                .select_folder(&mut conn, &folder, None, true)
                .await
                .map_err(account_error)?;
            let uidvalidity = selected
                .mailbox
                .uid_validity
                .ok_or_else(|| AccountError::Other("SELECT missing UIDVALIDITY".into()))?;
            let roots = conn
                .connection()
                .uid_thread(
                    "REFERENCES",
                    "UTF-8",
                    &plan.criteria,
                    account.command_timeout(),
                )
                .await
                .map_err(account_error)?;
            for root in roots {
                let mut uids = Vec::new();
                flatten_thread(&root, &mut uids);
                if !uids.is_empty() {
                    threads.push(encode_thread_id(&folder, uidvalidity, &uids));
                }
            }
        }
        page_from_items(threads, &request)
    })
}

pub(crate) fn search_messages(
    account: ImapAccount,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
    Box::pin(async move {
        let plan = search_plan(&request)?;
        let mut messages = Vec::new();
        for folder in search_folders(&account, plan.folder.as_ref()) {
            let mut conn = account
                .checkout_for_folder(&folder)
                .await
                .map_err(account_error)?;
            let selected = account
                .select_folder(&mut conn, &folder, None, true)
                .await
                .map_err(account_error)?;
            let uidvalidity = selected
                .mailbox
                .uid_validity
                .ok_or_else(|| AccountError::Other("SELECT missing UIDVALIDITY".into()))?;
            let result = conn
                .connection()
                .uid_search(&plan.criteria, account.command_timeout())
                .await
                .map_err(account_error)?;
            messages.extend(
                result
                    .ids
                    .into_iter()
                    .map(|uid| encode_object_id(&folder, uidvalidity, uid)),
            );
        }
        page_from_items(messages, &request)
    })
}

pub(crate) fn containers_list(
    account: ImapAccount,
) -> AccountFuture<Result<Vec<Container>, AccountError>> {
    Box::pin(async move { Ok(containers_snapshot(&account)) })
}

pub(crate) fn container_create(
    account: ImapAccount,
    kind: ContainerKind,
    name: String,
    parent: Option<ContainerId>,
) -> AccountFuture<Result<ContainerId, AccountError>> {
    Box::pin(async move {
        if !matches!(kind, ContainerKind::Folder) {
            return Err(AccountError::Unsupported);
        }
        let full_name = child_name(&account, parent.as_ref(), &name)?;
        let conn = account.pool.dial_idle().await.map_err(account_error)?;
        conn.create(full_name.as_str(), account.command_timeout())
            .await
            .map_err(account_error)?;
        refresh_folders(&account).await?;
        Ok(ContainerId(full_name.as_str().to_owned()))
    })
}

pub(crate) fn container_rename(
    account: ImapAccount,
    container: ContainerId,
    name: String,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let folder = folder_from_container(&container)?;
        let new_name = renamed_sibling(&account, &folder, &name)?;
        let conn = account.pool.dial_idle().await.map_err(account_error)?;
        conn.rename(
            folder.as_str(),
            new_name.as_str(),
            account.command_timeout(),
        )
        .await
        .map_err(account_error)?;
        refresh_folders(&account).await
    })
}

pub(crate) fn container_move(
    account: ImapAccount,
    container: ContainerId,
    new_parent: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let folder = folder_from_container(&container)?;
        let leaf = leaf_name(&account, &folder);
        let new_name = child_name(&account, new_parent.as_ref(), &leaf)?;
        let conn = account.pool.dial_idle().await.map_err(account_error)?;
        conn.rename(
            folder.as_str(),
            new_name.as_str(),
            account.command_timeout(),
        )
        .await
        .map_err(account_error)?;
        refresh_folders(&account).await
    })
}

pub(crate) fn container_delete(
    account: ImapAccount,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let folder = folder_from_container(&container)?;
        let conn = account.pool.dial_idle().await.map_err(account_error)?;
        let status = conn
            .status(folder.as_str(), "MESSAGES", account.command_timeout())
            .await
            .map_err(account_error)?;
        let non_empty = status
            .items
            .iter()
            .any(|item| matches!(item, StatusItem::Messages(count) if *count > 0));
        if non_empty {
            return Err(AccountError::Other(
                "refusing to delete non-empty IMAP mailbox".into(),
            ));
        }
        conn.delete(folder.as_str(), account.command_timeout())
            .await
            .map_err(account_error)?;
        refresh_folders(&account).await
    })
}

pub(crate) fn identities_list() -> AccountFuture<Result<Vec<Identity>, AccountError>> {
    Box::pin(async { Err(AccountError::Unsupported) })
}

pub(crate) fn identity_update(
    _identity: IdentityId,
    _patch: IdentityPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async { Err(AccountError::Unsupported) })
}

pub(crate) fn vacation_get() -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
    Box::pin(async { Err(AccountError::Unsupported) })
}

pub(crate) fn vacation_set(_config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async { Err(AccountError::Unsupported) })
}

pub(crate) fn quota_get(
    account: ImapAccount,
) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.quota_get {
            return Err(AccountError::Unsupported);
        }
        let Some(folder) = quota_probe_folder(&account) else {
            return Ok(None);
        };
        let conn = account.pool.dial_idle().await.map_err(account_error)?;
        let quota = conn
            .get_quota_root(folder.as_str(), account.command_timeout())
            .await
            .map_err(account_error)?;
        for (_root, resources) in quota.resources {
            for resource in resources {
                if resource.name.eq_ignore_ascii_case("STORAGE") {
                    return Ok(Some(QuotaInfo {
                        used_bytes: resource.usage.saturating_mul(1024),
                        total_bytes: Some(resource.limit.saturating_mul(1024)),
                    }));
                }
            }
        }
        Ok(None)
    })
}

pub(crate) fn thread_hydrate(
    account: ImapAccount,
    thread: ThreadId,
) -> AccountFuture<Result<ThreadHydration, AccountError>> {
    Box::pin(async move {
        let decoded = decode_thread_id(&thread)?;
        let messages = hydrate_decoded(
            &account,
            decoded
                .uids
                .iter()
                .map(|uid| DecodedObjectId {
                    folder: decoded.folder.clone(),
                    uidvalidity: decoded.uidvalidity,
                    uid: *uid,
                })
                .collect(),
            HydrationProjection::Full,
        )
        .await?;
        Ok(ThreadHydration {
            id: thread,
            messages,
        })
    })
}

pub(crate) fn message_hydrate(
    account: ImapAccount,
    message: ObjectId,
    projection: HydrationProjection,
) -> AccountFuture<Result<Message, AccountError>> {
    Box::pin(async move {
        let decoded = decode_object_id(&message)?;
        let mut messages = hydrate_decoded(&account, vec![decoded], projection).await?;
        messages
            .pop()
            .ok_or_else(|| AccountError::Other("message was not returned by IMAP FETCH".into()))
    })
}

pub(crate) fn move_thread(
    account: ImapAccount,
    thread: ThreadId,
    target: ContainerId,
    source: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        add_to_container(
            account.clone(),
            MutationTarget::Thread(thread.clone()),
            target,
        )
        .await?;
        if let Some(source) = source {
            remove_from_container(account, MutationTarget::Thread(thread), source).await?;
        }
        Ok(())
    })
}

pub(crate) fn delete_thread(
    account: ImapAccount,
    thread: ThreadId,
    current: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if let Some(current) = current {
            let folder = folder_from_container(&current)?;
            if folder_role(
                account
                    .folders
                    .get(&folder)
                    .as_deref()
                    .map(|entry| entry.attributes.as_slice())
                    .unwrap_or(&[]),
                folder.as_str(),
            ) == Some(FolderRole::Trash)
            {
                return remove_from_container(account, MutationTarget::Thread(thread), current)
                    .await;
            }
            let trash =
                role_folder(&account, FolderRole::Trash).ok_or(AccountError::Unsupported)?;
            return move_thread(
                account,
                thread,
                ContainerId(trash.as_str().to_owned()),
                Some(current),
            )
            .await;
        }
        let trash = role_folder(&account, FolderRole::Trash).ok_or(AccountError::Unsupported)?;
        move_thread(
            account,
            thread,
            ContainerId(trash.as_str().to_owned()),
            None,
        )
        .await
    })
}

async fn copy_messages(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    destination: &MailboxName,
) -> Result<(), AccountError> {
    for (folder, ids) in group_by_folder(ids) {
        let mut conn = account
            .checkout_for_folder(&folder)
            .await
            .map_err(account_error)?;
        let selected = account
            .select_folder(&mut conn, &folder, None, false)
            .await
            .map_err(account_error)?;
        let uidvalidity = selected
            .mailbox
            .uid_validity
            .ok_or_else(|| AccountError::Other("SELECT missing UIDVALIDITY".into()))?;
        let uids = valid_uids(ids, uidvalidity)?;
        let Some(uid_set) = uid_set_from_u32(&uids) else {
            continue;
        };
        conn.connection()
            .uid_copy(
                uid_set.as_sequence_set(),
                destination.as_str(),
                account.command_timeout(),
            )
            .await
            .map_err(account_error)?;
    }
    Ok(())
}

async fn delete_messages(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
) -> Result<(), AccountError> {
    for (folder, ids) in group_by_folder(ids) {
        let mut conn = account
            .checkout_for_folder(&folder)
            .await
            .map_err(account_error)?;
        let selected = account
            .select_folder(&mut conn, &folder, None, false)
            .await
            .map_err(account_error)?;
        let uidvalidity = selected
            .mailbox
            .uid_validity
            .ok_or_else(|| AccountError::Other("SELECT missing UIDVALIDITY".into()))?;
        let uids = valid_uids(ids, uidvalidity)?;
        let Some(uid_set) = uid_set_from_u32(&uids) else {
            continue;
        };
        conn.connection()
            .uid_store(
                uid_set.as_sequence_set(),
                StoreOperation::AddSilent,
                &[Flag::Deleted],
                None,
                account.command_timeout(),
            )
            .await
            .map_err(account_error)?;
        conn.connection()
            .uid_expunge(uid_set.as_sequence_set(), account.command_timeout())
            .await
            .map_err(account_error)?;
        account.folders.clear_modseqs(&folder, uidvalidity, &uids);
    }
    Ok(())
}

async fn set_flag(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    flag: Flag,
    value: bool,
) -> Result<(), AccountError> {
    let operation = if value {
        StoreOperation::AddSilent
    } else {
        StoreOperation::RemoveSilent
    };
    for (folder, ids) in group_by_folder(ids) {
        let mut conn = account
            .checkout_for_folder(&folder)
            .await
            .map_err(account_error)?;
        let selected = account
            .select_folder(&mut conn, &folder, None, false)
            .await
            .map_err(account_error)?;
        let uidvalidity = selected
            .mailbox
            .uid_validity
            .ok_or_else(|| AccountError::Other("SELECT missing UIDVALIDITY".into()))?;
        let uids = valid_uids(ids, uidvalidity)?;
        let Some(uid_set) = uid_set_from_u32(&uids) else {
            continue;
        };
        conn.connection()
            .uid_store(
                uid_set.as_sequence_set(),
                operation,
                std::slice::from_ref(&flag),
                None,
                account.command_timeout(),
            )
            .await
            .map_err(account_error)?;
        account.folders.clear_modseqs(&folder, uidvalidity, &uids);
    }
    Ok(())
}

async fn hydrate_decoded(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    projection: HydrationProjection,
) -> Result<Vec<Message>, AccountError> {
    let mut messages = Vec::new();
    for (folder, ids) in group_by_folder(ids) {
        let mut conn = account
            .checkout_for_folder(&folder)
            .await
            .map_err(account_error)?;
        let selected = account
            .select_folder(&mut conn, &folder, None, true)
            .await
            .map_err(account_error)?;
        let uidvalidity = selected
            .mailbox
            .uid_validity
            .ok_or_else(|| AccountError::Other("SELECT missing UIDVALIDITY".into()))?;
        let uids = valid_uids(ids, uidvalidity)?;
        let Some(uid_set) = uid_set_from_u32(&uids) else {
            continue;
        };
        let fetches = conn
            .connection()
            .uid_fetch(
                uid_set.as_sequence_set(),
                &attrs_for_hydration(projection),
                account.command_timeout(),
            )
            .await
            .map_err(account_error)?;
        for fetch in fetches {
            if let Some(message) = fetch_to_message(&folder, uidvalidity, fetch, projection) {
                messages.push(message);
            }
        }
    }
    messages.sort_by(|a, b| a.id.0.cmp(&b.id.0));
    Ok(messages)
}

fn decoded_targets(target: &MutationTarget) -> Result<Vec<DecodedObjectId>, AccountError> {
    match target {
        MutationTarget::Message(id) => Ok(vec![decode_object_id(id)?]),
        MutationTarget::Thread(thread) => {
            let decoded = decode_thread_id(thread)?;
            Ok(decoded
                .uids
                .into_iter()
                .map(|uid| DecodedObjectId {
                    folder: decoded.folder.clone(),
                    uidvalidity: decoded.uidvalidity,
                    uid,
                })
                .collect())
        }
        _ => Err(AccountError::Unsupported),
    }
}

fn group_by_folder(ids: Vec<DecodedObjectId>) -> Vec<(MailboxName, Vec<DecodedObjectId>)> {
    let mut grouped: HashMap<String, (MailboxName, Vec<DecodedObjectId>)> = HashMap::new();
    for id in ids {
        grouped
            .entry(id.folder.as_str().to_owned())
            .or_insert_with(|| (id.folder.clone(), Vec::new()))
            .1
            .push(id);
    }
    grouped.into_values().collect()
}

fn valid_uids(ids: Vec<DecodedObjectId>, uidvalidity: u32) -> Result<Vec<u32>, AccountError> {
    let mut uids = Vec::new();
    for id in ids {
        if id.uidvalidity != uidvalidity {
            return Err(AccountError::Other(
                "UIDVALIDITY changed before IMAP operation".into(),
            ));
        }
        uids.push(id.uid);
    }
    Ok(uids)
}

fn folder_from_container(container: &ContainerId) -> Result<MailboxName, AccountError> {
    MailboxName::new(container.0.clone()).map_err(|e| AccountError::Other(e.to_string()))
}

fn imap_flag_for_keyword(keyword: &str) -> Flag {
    if keyword.eq_ignore_ascii_case("$flagged") || keyword.eq_ignore_ascii_case("\\flagged") {
        Flag::Flagged
    } else if keyword.eq_ignore_ascii_case("$answered")
        || keyword.eq_ignore_ascii_case("\\answered")
    {
        Flag::Answered
    } else if keyword.eq_ignore_ascii_case("$seen") || keyword.eq_ignore_ascii_case("\\seen") {
        Flag::Seen
    } else {
        Flag::from(keyword)
    }
}

struct SearchPlan {
    criteria: String,
    folder: Option<MailboxName>,
}

fn search_plan(request: &SearchRequest) -> Result<SearchPlan, AccountError> {
    let mut plan = match &request.filter {
        Some(filter) => criteria_from_filter(filter)?,
        None => CriteriaPart {
            criteria: "ALL".to_owned(),
            folder: None,
        },
    };
    if let Some(raw) = request
        .provider_query
        .as_deref()
        .filter(|raw| !raw.trim().is_empty())
    {
        if plan.criteria == "ALL" {
            plan.criteria = raw.trim().to_owned();
        } else {
            plan.criteria.push(' ');
            plan.criteria.push_str(raw.trim());
        }
    }
    if plan.criteria.trim().is_empty() {
        plan.criteria = "ALL".to_owned();
    }
    Ok(SearchPlan {
        criteria: plan.criteria,
        folder: plan.folder,
    })
}

struct CriteriaPart {
    criteria: String,
    folder: Option<MailboxName>,
}

fn criteria_from_filter(filter: &SearchFilter) -> Result<CriteriaPart, AccountError> {
    match filter {
        SearchFilter::From(value) => leaf(SearchCriteria::new().from(value)),
        SearchFilter::To(value) => leaf(SearchCriteria::new().to(value)),
        SearchFilter::Subject(value) => leaf(SearchCriteria::new().subject(value)),
        SearchFilter::Body(value) | SearchFilter::Has(value) => {
            leaf(SearchCriteria::new().body(value))
        }
        SearchFilter::In(container) => Ok(CriteriaPart {
            criteria: "ALL".to_owned(),
            folder: Some(folder_from_container(container)?),
        }),
        SearchFilter::Labeled(LabelId(label)) => leaf(SearchCriteria::new().keyword(label)),
        SearchFilter::DateRange { after, before } => {
            let mut criteria = SearchCriteria::new();
            if let Some(after) = after {
                criteria = criteria
                    .sent_since(&imap_date(*after))
                    .map_err(|e| AccountError::Other(e.to_string()))?;
            }
            if let Some(before) = before {
                criteria = criteria
                    .sent_before(&imap_date(*before))
                    .map_err(|e| AccountError::Other(e.to_string()))?;
            }
            Ok(CriteriaPart {
                criteria: empty_to_all(criteria.as_str()),
                folder: None,
            })
        }
        SearchFilter::And(filters) => combine_and(filters),
        SearchFilter::Or(filters) => combine_or(filters),
        SearchFilter::Not(filter) => {
            let inner = criteria_from_filter(filter)?;
            Ok(CriteriaPart {
                criteria: format!("NOT ({})", inner.criteria),
                folder: inner.folder,
            })
        }
        _ => Err(AccountError::Unsupported),
    }
}

fn leaf(result: Result<SearchCriteria, crate::Error>) -> Result<CriteriaPart, AccountError> {
    let criteria = result.map_err(|e| AccountError::Other(e.to_string()))?;
    Ok(CriteriaPart {
        criteria: empty_to_all(criteria.as_str()),
        folder: None,
    })
}

fn combine_and(filters: &[SearchFilter]) -> Result<CriteriaPart, AccountError> {
    let mut criteria = Vec::new();
    let mut folder = None;
    for filter in filters {
        let part = criteria_from_filter(filter)?;
        folder = merge_folder(folder, part.folder)?;
        if part.criteria != "ALL" {
            criteria.push(part.criteria);
        }
    }
    Ok(CriteriaPart {
        criteria: if criteria.is_empty() {
            "ALL".to_owned()
        } else {
            criteria.join(" ")
        },
        folder,
    })
}

fn combine_or(filters: &[SearchFilter]) -> Result<CriteriaPart, AccountError> {
    if filters.is_empty() {
        return Ok(CriteriaPart {
            criteria: "ALL".to_owned(),
            folder: None,
        });
    }
    let mut parts = Vec::new();
    let mut folder = None;
    for filter in filters {
        let part = criteria_from_filter(filter)?;
        folder = merge_folder(folder, part.folder)?;
        parts.push(part.criteria);
    }
    let mut iter = parts.into_iter();
    let mut criteria = iter.next().unwrap_or_else(|| "ALL".to_owned());
    for next in iter {
        criteria = format!("OR ({criteria}) ({next})");
    }
    Ok(CriteriaPart { criteria, folder })
}

fn merge_folder(
    current: Option<MailboxName>,
    next: Option<MailboxName>,
) -> Result<Option<MailboxName>, AccountError> {
    match (current, next) {
        (Some(a), Some(b)) if a != b => Err(AccountError::Unsupported),
        (Some(a), _) => Ok(Some(a)),
        (_, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}

fn empty_to_all(criteria: &str) -> String {
    let trimmed = criteria.trim();
    if trimmed.is_empty() {
        "ALL".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn imap_date(time: SystemTime) -> String {
    let datetime: DateTime<Utc> = time.into();
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
    account
        .folders
        .entries()
        .into_iter()
        .filter(|entry| entry.selectable)
        .map(|entry| entry.name.clone())
        .collect()
}

fn page_from_items<T: Clone>(
    items: Vec<T>,
    request: &SearchRequest,
) -> Result<Page<T>, AccountError> {
    let offset = match &request.page_cursor {
        Some(cursor) => std::str::from_utf8(cursor)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or_else(|| AccountError::Other("invalid IMAP page cursor".into()))?,
        None => 0,
    };
    let limit = usize::try_from(request.limit.unwrap_or(500)).unwrap_or(usize::MAX);
    let end = offset.saturating_add(limit).min(items.len());
    let page_items = items.get(offset..end).unwrap_or(&[]).to_vec();
    let next_cursor = (end < items.len()).then(|| end.to_string().into_bytes());
    Ok(Page {
        items: page_items,
        next_cursor,
        estimated_total: Some(u64::try_from(items.len()).unwrap_or(u64::MAX)),
    })
}

fn flatten_thread(node: &ThreadNode, out: &mut Vec<u32>) {
    if let Some(uid) = node.id {
        out.push(uid);
    }
    for child in &node.children {
        flatten_thread(child, out);
    }
}

fn containers_snapshot(account: &ImapAccount) -> Vec<Container> {
    account
        .folders
        .entries()
        .into_iter()
        .map(|entry| {
            let native = entry.name.as_str().to_owned();
            Container {
                id: ContainerId(native.clone()),
                kind: ContainerKind::Folder,
                role: folder_role(&entry.attributes, &native),
                provenance: Provenance {
                    provider: ProtocolKind::Imap,
                    kind: ContainerKind::Folder,
                    native: native.clone(),
                },
                native_id: native.clone(),
                name: leaf_name(account, &entry.name),
                parent: parent_id(entry.delimiter, &native),
            }
        })
        .collect()
}

fn folder_role(attributes: &[MailboxAttribute], name: &str) -> Option<FolderRole> {
    for attr in attributes {
        match attr {
            MailboxAttribute::Sent => return Some(FolderRole::Sent),
            MailboxAttribute::Drafts => return Some(FolderRole::Drafts),
            MailboxAttribute::Archive => return Some(FolderRole::Archive),
            MailboxAttribute::Trash => return Some(FolderRole::Trash),
            MailboxAttribute::Junk => return Some(FolderRole::Spam),
            MailboxAttribute::Custom(value) if value.eq_ignore_ascii_case("\\Inbox") => {
                return Some(FolderRole::Inbox);
            }
            _ => {}
        }
    }
    let lower = name.rsplit('/').next().unwrap_or(name).to_ascii_lowercase();
    match lower.as_str() {
        "inbox" => Some(FolderRole::Inbox),
        "sent" | "sent mail" | "sent messages" => Some(FolderRole::Sent),
        "draft" | "drafts" => Some(FolderRole::Drafts),
        "archive" | "archives" => Some(FolderRole::Archive),
        "trash" | "deleted" | "deleted messages" => Some(FolderRole::Trash),
        "junk" | "spam" | "junk mail" => Some(FolderRole::Spam),
        _ => None,
    }
}

fn parent_id(delimiter: Option<char>, native: &str) -> Option<ContainerId> {
    let delimiter = delimiter?;
    native
        .rsplit_once(delimiter)
        .map(|(parent, _)| ContainerId(parent.to_owned()))
}

fn role_folder(account: &ImapAccount, role: FolderRole) -> Option<MailboxName> {
    account
        .folders
        .entries()
        .into_iter()
        .find(|entry| folder_role(&entry.attributes, entry.name.as_str()) == Some(role))
        .map(|entry| entry.name.clone())
}

fn quota_probe_folder(account: &ImapAccount) -> Option<MailboxName> {
    role_folder(account, FolderRole::Inbox).or_else(|| {
        account
            .folders
            .entries()
            .into_iter()
            .find(|entry| entry.selectable)
            .map(|entry| entry.name.clone())
    })
}

fn child_name(
    account: &ImapAccount,
    parent: Option<&ContainerId>,
    leaf: &str,
) -> Result<MailboxName, AccountError> {
    let Some(parent) = parent else {
        return MailboxName::new(leaf.to_owned()).map_err(|e| AccountError::Other(e.to_string()));
    };
    let parent_folder = folder_from_container(parent)?;
    let delimiter = account
        .folders
        .get(&parent_folder)
        .and_then(|entry| entry.delimiter)
        .ok_or(AccountError::Unsupported)?;
    MailboxName::new(format!("{}{delimiter}{leaf}", parent_folder.as_str()))
        .map_err(|e| AccountError::Other(e.to_string()))
}

fn renamed_sibling(
    account: &ImapAccount,
    folder: &MailboxName,
    new_leaf: &str,
) -> Result<MailboxName, AccountError> {
    let delimiter = account
        .folders
        .get(folder)
        .and_then(|entry| entry.delimiter);
    let Some(delimiter) = delimiter else {
        return MailboxName::new(new_leaf.to_owned())
            .map_err(|e| AccountError::Other(e.to_string()));
    };
    if let Some((parent, _)) = folder.as_str().rsplit_once(delimiter) {
        MailboxName::new(format!("{parent}{delimiter}{new_leaf}"))
            .map_err(|e| AccountError::Other(e.to_string()))
    } else {
        MailboxName::new(new_leaf.to_owned()).map_err(|e| AccountError::Other(e.to_string()))
    }
}

fn leaf_name(account: &ImapAccount, folder: &MailboxName) -> String {
    let delimiter = account
        .folders
        .get(folder)
        .and_then(|entry| entry.delimiter);
    delimiter
        .and_then(|delimiter| folder.as_str().rsplit_once(delimiter).map(|(_, leaf)| leaf))
        .unwrap_or_else(|| folder.as_str())
        .to_owned()
}

async fn refresh_folders(account: &ImapAccount) -> Result<(), AccountError> {
    let conn = account.pool.dial_idle().await.map_err(account_error)?;
    let profile = conn.server_profile();
    let folders = factory::list_folders(&conn, &account.config, &profile)
        .await
        .map_err(account_error)?;
    account.folders.replace_all(folders);
    Ok(())
}

fn attrs_for_hydration(projection: HydrationProjection) -> Vec<FetchAttr> {
    let mut attrs = vec![
        FetchAttr::Uid,
        FetchAttr::Flags,
        FetchAttr::Envelope,
        FetchAttr::Rfc822Size,
    ];
    match projection {
        HydrationProjection::Headers => {}
        HydrationProjection::Preview(limit) => attrs.push(FetchAttr::BodySection {
            peek: true,
            section: Some("TEXT".into()),
            partial: Some((0, u64::try_from(limit).unwrap_or(u64::MAX))),
        }),
        HydrationProjection::Full | HydrationProjection::FullWithBlobs => {
            attrs.push(FetchAttr::BodySection {
                peek: true,
                section: None,
                partial: None,
            });
        }
        _ => {}
    }
    attrs
}

fn fetch_to_message(
    folder: &MailboxName,
    uidvalidity: u32,
    fetch: crate::types::FetchResponse,
    projection: HydrationProjection,
) -> Option<Message> {
    let uid = fetch.uid?;
    let envelope = fetch.envelope.clone();
    let body = fetch
        .body_sections
        .iter()
        .find_map(|section| section.data.as_ref())
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned());
    Some(Message {
        id: encode_object_id(folder, uidvalidity, uid),
        thread_id: fetch
            .thread_id
            .map(ThreadId)
            .or_else(|| fetch.gmail_thread_id.map(|id| ThreadId(id.to_string()))),
        from: envelope
            .as_ref()
            .map(|env| addresses(&env.from))
            .unwrap_or_default(),
        to: envelope
            .as_ref()
            .map(|env| addresses(&env.to))
            .unwrap_or_default(),
        cc: envelope
            .as_ref()
            .map(|env| addresses(&env.cc))
            .unwrap_or_default(),
        bcc: envelope
            .as_ref()
            .map(|env| addresses(&env.bcc))
            .unwrap_or_default(),
        reply_to: envelope
            .as_ref()
            .map(|env| addresses(&env.reply_to))
            .unwrap_or_default(),
        subject: envelope.as_ref().and_then(|env| env.subject.clone()),
        date: None,
        containers: vec![ContainerId(folder.as_str().to_owned())],
        flags: super::inventory::flags_set(fetch.flags.as_deref().unwrap_or(&[])),
        body_text: match projection {
            HydrationProjection::Headers => None,
            _ => body.clone(),
        },
        body_html: None,
        attachments: Vec::new(),
        size_bytes: fetch.rfc822_size,
        in_reply_to: envelope
            .as_ref()
            .and_then(|env| env.first_in_reply_to().map(str::to_owned)),
        references: Vec::new(),
    })
}

fn addresses(addresses: &[crate::types::EnvelopeAddress]) -> Vec<Address> {
    addresses
        .iter()
        .filter_map(|addr| {
            addr.email().map(|address| Address {
                name: addr.name.clone(),
                address,
            })
        })
        .collect()
}

fn draft_patch_to_rfc5322(patch: &DraftPatch) -> Result<Vec<u8>, AccountError> {
    if patch
        .attachments_inline
        .as_ref()
        .is_some_and(|attachments| !attachments.is_empty())
        || patch
            .attachments_uploaded
            .as_ref()
            .is_some_and(|attachments| !attachments.is_empty())
    {
        return Err(AccountError::Unsupported);
    }
    let mut out = String::new();
    if let Some(Some(from)) = &patch.from {
        header(&mut out, "From", &format_address(from));
    }
    if let Some(to) = &patch.to {
        header(&mut out, "To", &format_addresses(to));
    }
    if let Some(cc) = &patch.cc {
        header(&mut out, "Cc", &format_addresses(cc));
    }
    if let Some(bcc) = &patch.bcc {
        header(&mut out, "Bcc", &format_addresses(bcc));
    }
    if let Some(reply_to) = &patch.reply_to {
        header(&mut out, "Reply-To", &format_addresses(reply_to));
    }
    if let Some(Some(subject)) = &patch.subject {
        header(&mut out, "Subject", subject);
    }
    if let Some(Some(in_reply_to)) = &patch.in_reply_to {
        header(&mut out, "In-Reply-To", in_reply_to);
    }
    if let Some(references) = &patch.references
        && !references.is_empty()
    {
        header(&mut out, "References", &references.join(" "));
    }
    out.push_str("MIME-Version: 1.0\r\n");
    match (&patch.body_text, &patch.body_html) {
        (Some(Some(text)), Some(Some(html))) => {
            let boundary = "bifrost-imap-draft-alt";
            header(
                &mut out,
                "Content-Type",
                &format!("multipart/alternative; boundary=\"{boundary}\""),
            );
            out.push_str("\r\n");
            out.push_str("--");
            out.push_str(boundary);
            out.push_str("\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n");
            out.push_str(text);
            out.push_str("\r\n--");
            out.push_str(boundary);
            out.push_str("\r\nContent-Type: text/html; charset=utf-8\r\n\r\n");
            out.push_str(html);
            out.push_str("\r\n--");
            out.push_str(boundary);
            out.push_str("--\r\n");
        }
        (_, Some(Some(html))) => {
            out.push_str("Content-Type: text/html; charset=utf-8\r\n\r\n");
            out.push_str(html);
        }
        (Some(Some(text)), _) => {
            out.push_str("Content-Type: text/plain; charset=utf-8\r\n\r\n");
            out.push_str(text);
        }
        _ => {
            out.push_str("Content-Type: text/plain; charset=utf-8\r\n\r\n");
        }
    }
    Ok(out.into_bytes())
}

fn header(out: &mut String, name: &str, value: &str) {
    if value.trim().is_empty() {
        return;
    }
    out.push_str(name);
    out.push_str(": ");
    out.push_str(&sanitize_header(value));
    out.push_str("\r\n");
}

fn sanitize_header(value: &str) -> String {
    value.replace(['\r', '\n'], " ")
}

fn format_addresses(addresses: &[Address]) -> String {
    addresses
        .iter()
        .map(format_address)
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_address(address: &Address) -> String {
    match &address.name {
        Some(name) if !name.trim().is_empty() => {
            format!(
                "{} <{}>",
                sanitize_header(name),
                sanitize_header(&address.address)
            )
        }
        _ => sanitize_header(&address.address),
    }
}
