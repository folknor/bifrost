use std::collections::HashMap;
use std::time::SystemTime;

use bifrost_types::cloud::HostedAttachment;
use bifrost_types::compose::{
    Address, AttachmentHandle, DraftHandle, DraftPatch, IdentityId, SendRequest,
};
use bifrost_types::container::{
    Container, ContainerId, ContainerKind, ContainerNamespace, ContainerRights, FolderRole,
    MutationTarget, Provenance,
};
use bifrost_types::hydration::{HydrationProjection, Importance, Message, ThreadHydration};
use bifrost_types::ids::{ObjectId, ThreadId};
use bifrost_types::page::Page;
use bifrost_types::search::{SearchFilter, SearchRequest};
use bifrost_types::settings::{Identity, IdentityPatch, QuotaInfo, VacationConfig};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountFuture, AccountOperation, Cause,
    DiagnosticText, LabelId, Protocol, ProtocolKind, RequestCause, RequestErrorKind,
};
use chrono::{DateTime, Datelike, Utc};

use crate::types::{
    AclRight, FetchAttr, Flag, MailboxAttribute, MailboxName, MailboxRights, SearchCriteria,
    StatusItem, StoreOperation, ThreadNode,
};

use super::{
    DecodedObjectId, ImapAccount, account_error_with, decode_object_id, decode_thread_id,
    encode_object_id, encode_thread_id, factory, uid_set_from_u32,
};
use crate::error::Error;

/// Build a `map_err` closure that stamps every internal IMAP `Error` with
/// the operation of the calling public method. Threading operation
/// per call site is what lets the central recovery mapping distinguish
/// `Reconcile` vs `Retry::SameRequest` on non-idempotent operations.
fn op_err(op: AccountOperation) -> impl Fn(Error) -> AccountError + Copy {
    move |e| account_error_with(e, super::error::ImapErrorContext::operation(op))
}

/// Memory budget for the one-shot full-message `BODY[]` fetch in
/// `draft_send`. A saved draft is the account's own outgoing message, so
/// this need only bound a single message body, not a folder sweep. 64 MiB
/// comfortably covers any realistic draft (large inline attachments
/// included) while keeping the `FetchLimit` guard engaged, so a corrupt or
/// adversarial server cannot make us buffer an unbounded literal.
const DRAFT_FETCH_BUDGET: usize = 64 * 1024 * 1024;

pub(crate) fn add_to_container(
    account: ImapAccount,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let destination = folder_from_container(&container)?;
        let ids = decoded_targets(&target)?;
        copy_messages(
            &account,
            ids,
            &destination,
            AccountOperation::AddToContainer,
        )
        .await
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
            return Err(super::error::unsupported(
                AccountOperation::RemoveFromContainer,
            ));
        }
        delete_messages(&account, ids, AccountOperation::RemoveFromContainer).await
    })
}

pub(crate) fn set_keyword(
    account: ImapAccount,
    target: MutationTarget,
    keyword: String,
    value: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.set_keyword {
            return Err(super::error::unsupported(AccountOperation::SetKeyword));
        }
        let flag = imap_flag_for_keyword(&keyword);
        let ids = decoded_targets(&target)?;
        set_flag(&account, ids, flag, value, AccountOperation::SetKeyword).await
    })
}

pub(crate) fn set_is_read(
    account: ImapAccount,
    target: MutationTarget,
    is_read: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.set_is_read {
            return Err(super::error::unsupported(AccountOperation::SetIsRead));
        }
        let ids = decoded_targets(&target)?;
        set_flag(
            &account,
            ids,
            Flag::Seen,
            is_read,
            AccountOperation::SetIsRead,
        )
        .await
    })
}

pub(crate) fn set_importance(
    account: ImapAccount,
    target: MutationTarget,
    level: Importance,
) -> AccountFuture<Result<(), AccountError>> {
    // IMAP has no native importance field; map onto the `$important`
    // keyword. Exclusive: `High` sets it, `Normal`/`Low` clear it - one
    // STORE op, never an expand-into-two.
    Box::pin(async move {
        if !account.capabilities.pim_methods.set_importance {
            return Err(super::error::unsupported(AccountOperation::SetImportance));
        }
        let important = importance_sets_important_keyword(level);
        let ids = decoded_targets(&target)?;
        set_flag(
            &account,
            ids,
            imap_flag_for_keyword("$important"),
            important,
            AccountOperation::SetImportance,
        )
        .await
    })
}

/// IMAP's two-valued importance mapping: `High` sets the `$important`
/// keyword, `Normal`/`Low` clear it.
fn importance_sets_important_keyword(level: Importance) -> bool {
    matches!(level, Importance::High)
}

/// IMAP/SMTP does not do Graph-style shared-mailbox send routing; a
/// `send_as` request is rejected `Unsupported(Send)` rather than
/// silently sent from the authenticated user's own mailbox.
fn send_as_guard(request: &SendRequest) -> Option<AccountError> {
    request
        .send_as
        .is_some()
        .then(|| super::error::unsupported(AccountOperation::Send))
}

pub(crate) fn unsupported_unit(
    operation: bifrost_types::AccountOperation,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move { Err(super::error::unsupported(operation)) })
}

pub(crate) fn unsupported_object(
    operation: bifrost_types::AccountOperation,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move { Err(super::error::unsupported(operation)) })
}

pub(crate) fn unsupported_attachment(
    operation: bifrost_types::AccountOperation,
) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
    Box::pin(async move { Err(super::error::unsupported(operation)) })
}

pub(crate) fn unsupported_hosted(
    operation: bifrost_types::AccountOperation,
) -> AccountFuture<Result<HostedAttachment, AccountError>> {
    Box::pin(async move { Err(super::error::unsupported(operation)) })
}

pub(crate) fn draft_create(
    account: ImapAccount,
    patch: DraftPatch,
) -> AccountFuture<Result<DraftHandle, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.draft_create {
            return Err(super::error::unsupported(AccountOperation::DraftCreate));
        }
        let folder = role_folder(&account, FolderRole::Drafts)
            .ok_or_else(|| super::error::unsupported(AccountOperation::DraftCreate))?;
        let raw = draft_patch_to_rfc5322(&patch)?;
        let err = op_err(AccountOperation::DraftCreate);
        let conn = account.pool.dial_idle().await.map_err(err)?;
        let appended = conn
            .append(
                folder.as_str(),
                &[Flag::Draft],
                None,
                &raw,
                account.command_timeout(),
            )
            .await
            .map_err(err)?;
        let Some((uidvalidity, uid)) = appended else {
            return Err(super::error::unsupported(AccountOperation::DraftCreate));
        };
        Ok(DraftHandle(encode_object_id(&folder, uidvalidity, uid).0))
    })
}

pub(crate) fn send_message(
    account: ImapAccount,
    request: SendRequest,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        // Graph-style shared-mailbox send routing is not modeled over
        // SMTP (a shared-mailbox send is `request.from` + relay
        // authorization). Reject a `send_as` before the submission check
        // so the contract stays uniform with the other providers.
        if let Some(err) = send_as_guard(&request) {
            return Err(err);
        }
        let Some(submission) = account.submission.clone() else {
            return Err(super::error::unsupported(AccountOperation::Send));
        };

        // The shared assembler is provider-neutral and leaves the protocol
        // unstamped; attribute it to IMAP here so a "no recipient" /
        // "uploaded attachments" rejection carries the right protocol.
        let rendered = bifrost_types::send_request_to_rfc5322(&request, submission.default_from())
            .map_err(stamp_imap_protocol)?;
        // Scheduled send rides on SMTP FUTURERELEASE (HOLDUNTIL). The
        // relay's EHLO at send time is authoritative: an unsupporting
        // relay surfaces an `Unsupported(Send)` AccountError from the
        // smtp boundary (kind set there, not here). Re-stamp only the
        // operation/protocol; `restamp` cannot change the kind.
        submission
            .send_rfc5322(&rendered.envelope, &rendered.raw, request.scheduled)
            .await
            .map_err(|err| restamp(err, AccountOperation::Send))?;

        // The message is committed. Optionally append to Sent; a failed
        // APPEND is non-fatal (never resend) but is not swallowed: it
        // surfaces the uncertain Sent state and falls back to the
        // generated Message-ID for the returned id.
        let save = request
            .save_to_sent
            .unwrap_or_else(|| submission.save_to_sent_default());
        // The Sent copy retains the `Bcc:` header (the sender's record of
        // who was blind copied); only the transmitted body strips it. The
        // assembler hands back a distinct Bcc-bearing body when a Bcc is
        // present, else the two are identical and we reuse `raw`.
        let sent_body = rendered.sent_copy.as_deref().unwrap_or(&rendered.raw);
        let object_id =
            append_to_sent_or_fallback(&account, sent_body, save, &rendered.message_id).await;
        Ok(object_id)
    })
}

pub(crate) fn send_raw_message(
    account: ImapAccount,
    raw: bytes::Bytes,
    save_to_sent: Option<bool>,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let Some(submission) = account.submission.clone() else {
            return Err(super::error::unsupported(AccountOperation::Send));
        };

        // The caller pre-assembled the RFC 5322 / RFC 8098 octets. Parse
        // the envelope (From/Sender drive MAIL FROM; To/Cc/Bcc drive RCPT
        // TO) out of the MIME headers and strip the Bcc header from the
        // transmitted body, exactly as draft_send does for a saved draft.
        let parsed = parse_draft_for_submission(&raw, submission.default_from())?;
        submission
            .send_rfc5322(&parsed.envelope, &parsed.body, None)
            .await
            .map_err(|err| restamp(err, AccountOperation::Send))?;

        // Committed. Optionally append to Sent. The Sent copy keeps the
        // verbatim caller bytes (Bcc header retained, the sender's record);
        // only `parsed.body` strips Bcc for the wire.
        let save = save_to_sent.unwrap_or_else(|| submission.save_to_sent_default());
        let object_id = append_to_sent_or_fallback(&account, &raw, save, &parsed.message_id).await;
        Ok(object_id)
    })
}

pub(crate) fn draft_send(
    account: ImapAccount,
    draft: DraftHandle,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let Some(submission) = account.submission.clone() else {
            return Err(super::error::unsupported(AccountOperation::DraftSend));
        };

        let decoded = decode_object_id(&ObjectId(draft.0.clone()))?;
        let raw = fetch_full_message(&account, &decoded).await?;

        // Parse the draft headers to build the envelope: To/Cc/Bcc drive
        // RCPT TO, From/Sender drive MAIL FROM. The Bcc header is folded
        // into recipients and stripped from the transmitted body so blind
        // recipients are delivered but never disclosed.
        let parsed = parse_draft_for_submission(&raw, submission.default_from())?;
        submission
            .send_rfc5322(&parsed.envelope, &parsed.body, None)
            .await
            .map_err(|err| restamp(err, AccountOperation::DraftSend))?;

        // Sent; discard the draft from Drafts and optionally append to
        // Sent. The Sent copy is the saved draft `raw`, which retains the
        // `Bcc:` header (the sender's record of who was blind copied) -
        // only `parsed.body` (the transmitted bytes) strips it.
        let object_id = append_to_sent_or_fallback(
            &account,
            &raw,
            submission.save_to_sent_default(),
            &parsed.message_id,
        )
        .await;

        // Discard the original draft. A failed discard is non-fatal: the
        // message was sent. Surface it as a warning rather than failing.
        if let Err(err) =
            delete_messages(&account, vec![decoded], AccountOperation::DraftSend).await
        {
            tracing::warn!(
                target: "bifrost_imap::draft_send",
                error = %err,
                "draft sent but discard from Drafts failed; the draft may linger"
            );
        }

        Ok(object_id)
    })
}

/// Append the raw message to the Sent folder when requested and a Sent
/// role folder resolves. Returns the real APPENDUID-derived `ObjectId`
/// when the server surfaces one under UIDPLUS, else the supplied
/// fallback `Message-ID` (controlled domain). A failed APPEND after a
/// committed send is non-fatal: the send already succeeded, so we never
/// re-drive SMTP; the uncertain Sent state is logged for reconcile.
async fn append_to_sent_or_fallback(
    account: &ImapAccount,
    raw: &[u8],
    save: bool,
    fallback_message_id: &str,
) -> ObjectId {
    let fallback = || ObjectId(format!("imapmsgid1:{fallback_message_id}"));
    if !save {
        return fallback();
    }
    let Some(sent) = role_folder(account, FolderRole::Sent) else {
        tracing::warn!(
            target: "bifrost_imap::send",
            "save_to_sent requested but no Sent folder resolved; returning generated Message-ID"
        );
        return fallback();
    };
    let conn = match account.pool.dial_idle().await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::warn!(
                target: "bifrost_imap::send",
                error = %err,
                "message sent but Sent-folder APPEND could not get a connection; \
                 local Sent view is uncertain (reconcile by checking the Sent folder)"
            );
            return fallback();
        }
    };
    match conn
        .append(
            sent.as_str(),
            &[Flag::Seen],
            None,
            raw,
            account.command_timeout(),
        )
        .await
    {
        Ok(Some((uidvalidity, uid))) => encode_object_id(&sent, uidvalidity, uid),
        Ok(None) => {
            // Sent succeeded but the server gave no UIDPLUS code.
            fallback()
        }
        Err(err) => {
            tracing::warn!(
                target: "bifrost_imap::send",
                error = %err,
                "message sent but Sent-folder APPEND failed; \
                 local Sent view is uncertain (reconcile by checking the Sent folder)"
            );
            fallback()
        }
    }
}

/// One-shot raw `BODY[]` full-message fetch. Hydration returns a parsed
/// projection, not verbatim octets, so `draft_send` needs this dedicated
/// fetch to recover the exact RFC 5322 bytes of a saved draft.
async fn fetch_full_message(
    account: &ImapAccount,
    decoded: &DecodedObjectId,
) -> Result<Vec<u8>, AccountError> {
    let err = op_err(AccountOperation::DraftSend);
    let mut conn = account
        .checkout_for_folder(&decoded.folder)
        .await
        .map_err(err)?;
    account
        .select_folder(&mut conn, &decoded.folder, None, true)
        .await
        .map_err(err)?;
    let uids = uid_set_from_u32(&[decoded.uid])
        .ok_or_else(|| pim_malformed("draft object id has no UID"))?;
    let responses = conn
        .connection()
        .uid_fetch_full_messages(&uids, DRAFT_FETCH_BUDGET, account.command_timeout())
        .await
        .map_err(err)?;
    responses
        .into_iter()
        .find(|fetch| fetch.uid == Some(decoded.uid))
        .and_then(|fetch| {
            fetch
                .body_sections
                .into_iter()
                .find(|section| section.section.is_empty())
                .and_then(|section| section.data)
        })
        .ok_or_else(|| pim_malformed("draft message body not returned by FETCH"))
}

#[derive(Debug)]
struct ParsedDraft {
    envelope: bifrost_types::SubmissionEnvelope,
    body: Vec<u8>,
    message_id: String,
}

/// Parse a saved draft's RFC 5322 headers into a submission envelope and
/// strip the `Bcc:` header from the transmitted body. To/Cc/Bcc become
/// the recipient set; From (or `default_from`) is the reverse path. The
/// draft's own `Message-ID` is the fallback id.
fn parse_draft_for_submission(
    raw: &[u8],
    default_from: &bifrost_types::Address,
) -> Result<ParsedDraft, AccountError> {
    // Operate on bytes end-to-end: the transmitted body must be the
    // verbatim draft octets. Decoding through `from_utf8_lossy` would
    // turn any 8-bit / binary octet into U+FFFD on the wire while the
    // Sent-folder APPEND keeps the original bytes - a silent divergence
    // between what was sent and what is recorded. Header parsing only
    // needs the names and the address-list / message-id values, which
    // are ASCII-structured; we decode those lossily for parsing but
    // never feed the decoded form back into the transmitted bytes.
    let (header_end, body_start) = split_header_body(raw);
    let header_block = &raw[..header_end];

    let headers = unfold_headers(&String::from_utf8_lossy(header_block));

    let from = first_address(&headers, "from")
        .or_else(|| first_address(&headers, "sender"))
        .unwrap_or_else(|| default_from.clone());

    let mut recipients = Vec::new();
    recipients.extend(addresses_for(&headers, "to"));
    recipients.extend(addresses_for(&headers, "cc"));
    recipients.extend(addresses_for(&headers, "bcc"));
    if recipients.is_empty() {
        return Err(pim_malformed("draft has no recipients"));
    }

    let message_id = header_value(&headers, "message-id")
        .map(|value| {
            value
                .trim()
                .trim_matches(|c| c == '<' || c == '>')
                .to_owned()
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            let domain = from
                .address
                .rsplit_once('@')
                .map_or("localhost", |(_, domain)| domain);
            format!("bifrost.draft@{domain}")
        });

    let body = strip_bcc_header(header_block, &raw[body_start..]);

    Ok(ParsedDraft {
        envelope: bifrost_types::SubmissionEnvelope { from, recipients },
        body,
        message_id,
    })
}

/// Locate the header/body split of a raw RFC 5322 message, tolerating
/// mixed CRLF / bare-LF line endings and a header-only draft (no blank
/// line). Returns `(header_end, body_start)` byte offsets: the header
/// block is `raw[..header_end]` and the body is `raw[body_start..]`. The
/// separator blank line itself sits between the two and is dropped. For
/// a header-only draft both offsets are `raw.len()` (empty body).
fn split_header_body(raw: &[u8]) -> (usize, usize) {
    // Find the empty line that ends the header block. We scan for the
    // first line terminator (`\r\n` or bare `\n`) that is immediately
    // followed by another line terminator. `header_end` is the offset
    // just *after* the first terminator (so the last header line keeps
    // its own ending); `body_start` is just *after* the second
    // terminator (so the blank separator line is dropped). Mixed CRLF /
    // bare-LF endings are tolerated on both sides.
    let mut i = 0;
    while i < raw.len() {
        // Length of a terminator starting at `j`, if any.
        let term_len = |j: usize| -> Option<usize> {
            match raw.get(j) {
                Some(b'\r') if raw.get(j + 1) == Some(&b'\n') => Some(2),
                Some(b'\n') => Some(1),
                _ => None,
            }
        };
        if let Some(first) = term_len(i) {
            let header_end = i + first;
            if let Some(second) = term_len(header_end) {
                return (header_end, header_end + second);
            }
        }
        i += 1;
    }
    (raw.len(), raw.len())
}

/// Reassemble the message bytes with every `Bcc:` header line removed so
/// blind recipients are not disclosed in the transmitted message.
///
/// Byte-exact: header and body octets are copied verbatim (no UTF-8
/// round-trip), so an 8-bit / binary body survives unchanged. Only the
/// dropped `Bcc:` lines and the synthesized header/body separator touch
/// line endings; the separator matches the header block's own ending
/// (CRLF unless the draft is bare-LF) so a uniform-LF draft is not
/// emitted with a mixed CRLF separator.
fn strip_bcc_header(header_block: &[u8], body: &[u8]) -> Vec<u8> {
    let uses_crlf = header_block.windows(2).any(|w| w == b"\r\n") || !header_block.contains(&b'\n');
    let sep: &[u8] = if uses_crlf { b"\r\n" } else { b"\n" };

    let mut kept = Vec::with_capacity(header_block.len() + body.len() + 4);
    let mut skipping = false;
    for line in split_inclusive_lf(header_block) {
        let trimmed = trim_leading_crlf(line);
        let is_continuation = line.first() == Some(&b' ') || line.first() == Some(&b'\t');
        if !is_continuation {
            skipping = header_name_is_bcc(trimmed);
        }
        if !skipping {
            kept.extend_from_slice(line);
        }
    }
    // Ensure the header block ends with a line terminator, then add the
    // blank-line separator, then the verbatim body.
    if !kept.ends_with(b"\n") {
        kept.extend_from_slice(sep);
    }
    kept.extend_from_slice(sep);
    kept.extend_from_slice(body);
    kept
}

/// Split on `\n`, keeping the terminator on each line (the byte analogue
/// of `str::split_inclusive('\n')`), and skipping a trailing empty slice.
fn split_inclusive_lf(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut start = 0;
    std::iter::from_fn(move || {
        if start >= bytes.len() {
            return None;
        }
        let rest = &bytes[start..];
        match rest.iter().position(|&b| b == b'\n') {
            Some(idx) => {
                let line = &rest[..=idx];
                start += idx + 1;
                Some(line)
            }
            None => {
                let line = rest;
                start = bytes.len();
                Some(line)
            }
        }
    })
}

fn trim_leading_crlf(line: &[u8]) -> &[u8] {
    let mut s = line;
    while let [first, rest @ ..] = s
        && (*first == b'\r' || *first == b'\n')
    {
        s = rest;
    }
    s
}

/// True when a header line's field name (before the first `:`) is `Bcc`,
/// case-insensitive, ignoring surrounding whitespace.
fn header_name_is_bcc(line: &[u8]) -> bool {
    match line.iter().position(|&b| b == b':') {
        Some(idx) => line[..idx].trim_ascii().eq_ignore_ascii_case(b"bcc"),
        None => false,
    }
}

/// Collapse RFC 5322 folded header lines into `(lowercase-name, value)`
/// pairs, preserving order.
fn unfold_headers(header_block: &str) -> Vec<(String, String)> {
    let mut headers: Vec<(String, String)> = Vec::new();
    for line in header_block.split('\n') {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        if (line.starts_with(' ') || line.starts_with('\t')) && !headers.is_empty() {
            let last = headers.last_mut().expect("non-empty");
            last.1.push(' ');
            last.1.push_str(line.trim());
        } else if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
        }
    }
    headers
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

fn addresses_for(headers: &[(String, String)], name: &str) -> Vec<bifrost_types::Address> {
    headers
        .iter()
        .filter(|(key, _)| key == name)
        .flat_map(|(_, value)| parse_address_list(value))
        .collect()
}

fn first_address(headers: &[(String, String)], name: &str) -> Option<bifrost_types::Address> {
    header_value(headers, name).and_then(|value| parse_address_list(value).into_iter().next())
}

/// Minimal RFC 5322 address-list parser sufficient for envelope
/// construction (recipient addr-specs and an optional display name).
///
/// Commas inside a quoted display name or inside the angle-bracketed
/// addr-spec do not split addresses - otherwise a draft this crate
/// itself emitted (the shared assembler quotes names like
/// `"Last, First" <a@b>`) would round-trip into garbage recipients.
fn parse_address_list(value: &str) -> Vec<bifrost_types::Address> {
    split_address_list(value)
        .into_iter()
        .filter_map(|item| {
            let item = item.trim();
            if item.is_empty() {
                return None;
            }
            if let Some((name, rest)) = item.split_once('<')
                && let Some((address, _)) = rest.split_once('>')
            {
                let name = name.trim().trim_matches('"').trim();
                return Some(bifrost_types::Address {
                    name: (!name.is_empty()).then(|| name.to_owned()),
                    address: address.trim().to_owned(),
                });
            }
            Some(bifrost_types::Address::bare(item.to_owned()))
        })
        .collect()
}

/// Split an address-list header value on the top-level commas only:
/// commas inside a `"..."` quoted display name or inside a `<...>`
/// addr-spec are not separators.
fn split_address_list(value: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut in_angle = false;
    for ch in value.chars() {
        match ch {
            '"' if !in_angle => {
                in_quotes = !in_quotes;
                current.push(ch);
            }
            '<' if !in_quotes => {
                in_angle = true;
                current.push(ch);
            }
            '>' if !in_quotes => {
                in_angle = false;
                current.push(ch);
            }
            ',' if !in_quotes && !in_angle => {
                items.push(std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
    }
    items.push(current);
    items
}

/// Re-stamp an `AccountError` from the submission boundary with the IMAP
/// operation that drove it, so recovery/telemetry attribute it correctly.
///
/// Coupling caveat: `into_builder` does NOT preserve builder-only
/// overrides (`idempotency_override`, `throttle_scope`) per
/// `reference/error-model.md`. After the round-trip, `derive` recomputes
/// idempotency from the *operation* via `AccountOperation::is_idempotent`.
/// This is correct today only because both operations `restamp` is called
/// with - `Send` and `DraftSend` - are non-idempotent in the central
/// table, matching what an SMTP `idempotency_override(false)` would have
/// said. If a future caller passes an idempotent operation here, or if
/// SMTP ever stamps an `idempotency_override` that disagrees with the
/// operation's default, this re-stamp would silently drop it. The fix
/// would be to reapply the override after `into_builder` (it is readable
/// from the original error's recovery only indirectly), which requires
/// either an accessor in `bifrost-types` or threading the override
/// through - out of this crate's scope.
fn restamp(err: AccountError, op: AccountOperation) -> AccountError {
    err.into_builder()
        .operation(op)
        .try_build()
        .expect("valid account error classification")
}

/// Attribute a provider-neutral assembler error to IMAP. The shared
/// `bifrost-types` MIME assembler leaves the protocol unset (it is shared
/// across providers); the IMAP send path stamps `Protocol::Imap` so
/// telemetry and recovery attribute the failure to this account.
fn stamp_imap_protocol(err: AccountError) -> AccountError {
    err.into_builder()
        .protocol(Protocol::Imap)
        .try_build()
        .expect("valid account error classification")
}

pub(crate) fn draft_discard(
    account: ImapAccount,
    draft: DraftHandle,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let id = decode_object_id(&ObjectId(draft.0))?;
        delete_messages(&account, vec![id], AccountOperation::DraftDiscard).await
    })
}

pub(crate) fn search(
    account: ImapAccount,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.search {
            return Err(super::error::unsupported(AccountOperation::Search));
        }
        let err = op_err(AccountOperation::Search);
        let plan = search_plan(&request)?;
        let mut threads = Vec::new();
        for folder in search_folders(&account, plan.folder.as_ref()) {
            let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
            let selected = account
                .select_folder(&mut conn, &folder, None, true)
                .await
                .map_err(err)?;
            let uidvalidity = selected
                .mailbox
                .uid_validity
                .ok_or_else(|| pim_malformed("SELECT missing UIDVALIDITY"))?;
            let roots = conn
                .connection()
                .uid_thread(
                    "REFERENCES",
                    "UTF-8",
                    &plan.criteria,
                    account.command_timeout(),
                )
                .await
                .map_err(err)?;
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
        if !account.capabilities.pim_methods.search_messages {
            return Err(super::error::unsupported(AccountOperation::SearchMessages));
        }
        let err = op_err(AccountOperation::SearchMessages);
        let plan = search_plan(&request)?;
        let mut messages = Vec::new();
        for folder in search_folders(&account, plan.folder.as_ref()) {
            let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
            let selected = account
                .select_folder(&mut conn, &folder, None, true)
                .await
                .map_err(err)?;
            let uidvalidity = selected
                .mailbox
                .uid_validity
                .ok_or_else(|| pim_malformed("SELECT missing UIDVALIDITY"))?;
            let result = conn
                .connection()
                .uid_search(&plan.criteria, account.command_timeout())
                .await
                .map_err(err)?;
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
    // IMAP mailboxes carry no container color; accepted for trait
    // parity with the colorable (Gmail) path and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> AccountFuture<Result<ContainerId, AccountError>> {
    Box::pin(async move {
        if !matches!(kind, ContainerKind::Folder) {
            return Err(super::error::unsupported(AccountOperation::ContainerCreate));
        }
        let full_name = child_name(&account, parent.as_ref(), &name)?;
        let err = op_err(AccountOperation::ContainerCreate);
        let conn = account.pool.dial_idle().await.map_err(err)?;
        conn.create(full_name.as_str(), account.command_timeout())
            .await
            .map_err(err)?;
        refresh_folders(&account, AccountOperation::ContainerCreate).await?;
        Ok(ContainerId(full_name.as_str().to_owned()))
    })
}

pub(crate) fn container_rename(
    account: ImapAccount,
    container: ContainerId,
    name: String,
    // IMAP has no mailbox recolor; accepted for trait parity and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let folder = folder_from_container(&container)?;
        let new_name = renamed_sibling(&account, &folder, &name)?;
        let err = op_err(AccountOperation::ContainerRename);
        let conn = account.pool.dial_idle().await.map_err(err)?;
        conn.rename(
            folder.as_str(),
            new_name.as_str(),
            account.command_timeout(),
        )
        .await
        .map_err(err)?;
        refresh_folders(&account, AccountOperation::ContainerRename).await
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
        let err = op_err(AccountOperation::ContainerMove);
        let conn = account.pool.dial_idle().await.map_err(err)?;
        conn.rename(
            folder.as_str(),
            new_name.as_str(),
            account.command_timeout(),
        )
        .await
        .map_err(err)?;
        refresh_folders(&account, AccountOperation::ContainerMove).await
    })
}

pub(crate) fn container_delete(
    account: ImapAccount,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let folder = folder_from_container(&container)?;
        let err = op_err(AccountOperation::ContainerDelete);
        let conn = account.pool.dial_idle().await.map_err(err)?;
        let status = conn
            .status(folder.as_str(), "MESSAGES", account.command_timeout())
            .await
            .map_err(err)?;
        let non_empty = status
            .items
            .iter()
            .any(|item| matches!(item, StatusItem::Messages(count) if *count > 0));
        if non_empty {
            return Err(pim_malformed("refusing to delete non-empty IMAP mailbox"));
        }
        conn.delete(folder.as_str(), account.command_timeout())
            .await
            .map_err(err)?;
        refresh_folders(&account, AccountOperation::ContainerDelete).await
    })
}

pub(crate) fn identities_list() -> AccountFuture<Result<Vec<Identity>, AccountError>> {
    Box::pin(async { Err(super::error::unsupported(AccountOperation::IdentitiesList)) })
}

pub(crate) fn identity_update(
    _identity: IdentityId,
    _patch: IdentityPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async { Err(super::error::unsupported(AccountOperation::IdentityUpdate)) })
}

pub(crate) fn vacation_get() -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
    Box::pin(async { Err(super::error::unsupported(AccountOperation::VacationGet)) })
}

pub(crate) fn vacation_set(_config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async { Err(super::error::unsupported(AccountOperation::VacationSet)) })
}

pub(crate) fn quota_get(
    account: ImapAccount,
) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.quota_get {
            return Err(super::error::unsupported(AccountOperation::QuotaGet));
        }
        let Some(folder) = quota_probe_folder(&account) else {
            return Ok(None);
        };
        let err = op_err(AccountOperation::QuotaGet);
        let conn = account.pool.dial_idle().await.map_err(err)?;
        let quota = conn
            .get_quota_root(folder.as_str(), account.command_timeout())
            .await
            .map_err(err)?;
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
            AccountOperation::HydrateThread,
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
        let mut messages = hydrate_decoded(
            &account,
            vec![decoded],
            projection,
            AccountOperation::HydrateMessage,
        )
        .await?;
        messages
            .pop()
            .ok_or_else(|| pim_malformed("message was not returned by IMAP FETCH"))
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
            let trash = role_folder(&account, FolderRole::Trash)
                .ok_or_else(|| super::error::unsupported(AccountOperation::BulkMove))?;
            return move_thread(
                account,
                thread,
                ContainerId(trash.as_str().to_owned()),
                Some(current),
            )
            .await;
        }
        let trash = role_folder(&account, FolderRole::Trash)
            .ok_or_else(|| super::error::unsupported(AccountOperation::BulkMove))?;
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
    op: AccountOperation,
) -> Result<(), AccountError> {
    let err = op_err(op);
    for (folder, ids) in group_by_folder(ids) {
        let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
        let selected = account
            .select_folder(&mut conn, &folder, None, false)
            .await
            .map_err(err)?;
        let uidvalidity = selected
            .mailbox
            .uid_validity
            .ok_or_else(|| pim_malformed("SELECT missing UIDVALIDITY"))?;
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
            .map_err(err)?;
    }
    Ok(())
}

async fn delete_messages(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let err = op_err(op);
    for (folder, ids) in group_by_folder(ids) {
        let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
        let selected = account
            .select_folder(&mut conn, &folder, None, false)
            .await
            .map_err(err)?;
        let uidvalidity = selected
            .mailbox
            .uid_validity
            .ok_or_else(|| pim_malformed("SELECT missing UIDVALIDITY"))?;
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
            .map_err(err)?;
        conn.connection()
            .uid_expunge(uid_set.as_sequence_set(), account.command_timeout())
            .await
            .map_err(err)?;
        account.folders.clear_modseqs(&folder, uidvalidity, &uids);
    }
    Ok(())
}

async fn set_flag(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    flag: Flag,
    value: bool,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let err = op_err(op);
    let operation = if value {
        StoreOperation::AddSilent
    } else {
        StoreOperation::RemoveSilent
    };
    for (folder, ids) in group_by_folder(ids) {
        let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
        let selected = account
            .select_folder(&mut conn, &folder, None, false)
            .await
            .map_err(err)?;
        let uidvalidity = selected
            .mailbox
            .uid_validity
            .ok_or_else(|| pim_malformed("SELECT missing UIDVALIDITY"))?;
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
            .map_err(err)?;
        account.folders.clear_modseqs(&folder, uidvalidity, &uids);
    }
    Ok(())
}

async fn hydrate_decoded(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    projection: HydrationProjection,
    op: AccountOperation,
) -> Result<Vec<Message>, AccountError> {
    let err = op_err(op);
    let mut messages = Vec::new();
    for (folder, ids) in group_by_folder(ids) {
        let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
        let selected = account
            .select_folder(&mut conn, &folder, None, true)
            .await
            .map_err(err)?;
        let uidvalidity = selected
            .mailbox
            .uid_validity
            .ok_or_else(|| pim_malformed("SELECT missing UIDVALIDITY"))?;
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
            .map_err(err)?;
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
        _ => Err(pim_malformed(
            "unsupported MutationTarget variant for IMAP per-message operation",
        )),
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
            return Err(pim_malformed("UIDVALIDITY changed before IMAP operation"));
        }
        uids.push(id.uid);
    }
    Ok(uids)
}

fn folder_from_container(container: &ContainerId) -> Result<MailboxName, AccountError> {
    MailboxName::new(container.0.clone()).map_err(|e| pim_malformed(e.to_string()))
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

#[derive(Debug)]
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
                    .map_err(|e| pim_malformed(e.to_string()))?;
            }
            if let Some(before) = before {
                criteria = criteria
                    .sent_before(&imap_date(*before))
                    .map_err(|e| pim_malformed(e.to_string()))?;
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
        _ => Err(super::error::unsupported(AccountOperation::Search)),
    }
}

fn leaf(result: Result<SearchCriteria, crate::Error>) -> Result<CriteriaPart, AccountError> {
    let criteria = result.map_err(|e| pim_malformed(e.to_string()))?;
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
    for filter in filters {
        let part = criteria_from_filter(filter)?;
        // A folder restriction (`In`) nested inside an `Or` cannot be
        // expressed in a single-mailbox IMAP SEARCH: SEARCH runs against
        // the currently-selected mailbox, so hoisting the `In` folder to
        // the whole OR group would silently restrict the *other* OR
        // branches to that folder too, changing the query's meaning. The
        // AND case is defensible (the folder narrows the whole
        // conjunction); the OR case is not, so reject it explicitly
        // rather than leak the restriction.
        if part.folder.is_some() {
            return Err(or_folder_restriction_error());
        }
        parts.push(part.criteria);
    }
    let folder = None;
    let mut iter = parts.into_iter();
    let mut criteria = iter.next().unwrap_or_else(|| "ALL".to_owned());
    for next in iter {
        criteria = format!("OR ({criteria}) ({next})");
    }
    Ok(CriteriaPart { criteria, folder })
}

#[allow(clippy::unwrap_in_result)]
fn merge_folder(
    current: Option<MailboxName>,
    next: Option<MailboxName>,
) -> Result<Option<MailboxName>, bifrost_types::AccountError> {
    match (current, next) {
        (Some(a), Some(b)) if a != b => {
            use bifrost_types::{
                AccountErrorBuilder, AccountErrorKind, Cause, DiagnosticText, Protocol,
                RequestCause, RequestErrorKind,
            };
            Err(AccountErrorBuilder::new(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::InvalidArgument {
                    field: Some("folder"),
                    message: Some(DiagnosticText::support_only(format!(
                        "search filter spans two folders ({} and {}); \
                         IMAP SEARCH cannot span multiple mailboxes in one command",
                        a.as_str(),
                        b.as_str()
                    ))),
                }),
            )
            .protocol(Protocol::Imap)
            .operation(bifrost_types::AccountOperation::SearchMessages)
            .try_build()
            .expect("valid account error classification"))
        }
        (Some(a), _) => Ok(Some(a)),
        (_, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}

/// A folder restriction (`SearchFilter::In`) nested inside an `Or` has no
/// faithful single-mailbox IMAP SEARCH encoding (see `combine_or`).
fn or_folder_restriction_error() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("folder"),
            message: Some(DiagnosticText::support_only(
                "a folder restriction (In) inside an Or cannot be expressed as a \
                 single-mailbox IMAP SEARCH; restructure the query so folder scoping \
                 is not OR-ed with other criteria",
            )),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::SearchMessages)
    .try_build()
    .expect("valid account error classification")
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
    // `entries()` iterates a `HashMap`, so its order is not stable
    // run-to-run. The multi-folder search cursor is a plain offset into
    // the concatenated per-folder result list, so a nondeterministic
    // folder order would make the same `page_cursor` resolve to a
    // different slice on the next page request (dropped / duplicated
    // results across pages). Sort by mailbox name so the concatenation
    // order - and therefore the offset cursor - is stable.
    let mut folders: Vec<MailboxName> = account
        .folders
        .entries()
        .into_iter()
        .filter(|entry| entry.selectable)
        .map(|entry| entry.name.clone())
        .collect();
    folders.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    folders
}

fn page_from_items<T: Clone>(
    items: Vec<T>,
    request: &SearchRequest,
) -> Result<Page<T>, AccountError> {
    let offset = match &request.page_cursor {
        Some(cursor) => std::str::from_utf8(cursor)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or_else(|| pim_malformed("invalid IMAP page cursor"))?,
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
        failed_ids: Vec::new(),
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
            let display_name = leaf_name(account, &entry.name);
            container_from_folder_entry(&entry, display_name)
        })
        .collect()
}

/// Project one registry entry onto a `Container`. Pure over the entry plus
/// the already-resolved display name (the only piece that needs the
/// account, for INBOX aliasing), so the namespace / owner / rights
/// projection is unit-pinnable without a live connection.
///
/// A shared/other-user folder is namespaced by its owning mailbox. IMAP has
/// no separate per-owner id space - the full mailbox path IS the native id
/// in every namespace - so `owner_local_id` is that same path, and
/// `native_id` (which the cursor scope keys on) needs no re-encoding. That
/// keeps `native_id` byte-identical to the `CursorScope::Folder` string
/// `discover_cursor_scopes` emits for the same folder.
fn container_from_folder_entry(
    entry: &super::folder_registry::FolderEntry,
    display_name: String,
) -> Container {
    let native = entry.name.as_str().to_owned();
    let namespace = if entry.shared_owner.is_some() {
        ContainerNamespace::Shared
    } else {
        ContainerNamespace::Personal
    };
    Container::new(
        ContainerId(native.clone()),
        ContainerKind::Folder,
        folder_role(&entry.attributes, &native),
        Provenance {
            provider: ProtocolKind::Imap,
            kind: ContainerKind::Folder,
            native: native.clone(),
        },
        display_name,
        parent_id(entry.delimiter, &native),
    )
    // IMAP mailboxes carry no container color, and IMAP is folder-shaped
    // (special-use maps into `role`), so `style` and `system` keep their
    // `Container::new` defaults.
    .with_namespace(namespace)
    .with_owner(entry.shared_owner.clone())
    .with_owner_local_id(entry.shared_owner.as_ref().map(|_| native.clone()))
    // RFC 4314 MYRIGHTS, captured at shared-folder discovery. `None` for a
    // personal folder or a server without ACL.
    .with_rights(entry.rights.as_ref().map(rights_from_myrights))
}

/// Project an RFC 4314 MYRIGHTS set onto the unified [`ContainerRights`].
///
/// Every member is `Some(_)`: once the server answered MYRIGHTS, the
/// absence of a letter is a definite "no", not "unreported" (the
/// unreported case is `FolderEntry::rights == None`, which never reaches
/// here). The mapping follows RFC 4314 Section 4:
///
/// - `l`+`r` -> read (`l` is the visibility gate, `r` the access gate)
/// - `i` -> add items (APPEND / COPY into)
/// - `t` -> remove items (set `\Deleted`)
/// - `s` -> persist `\Seen`
/// - `w` -> set other flags/keywords
/// - `k` (or the pre-4314 `c`) -> create a child mailbox
/// - `x` (or the pre-4314 `d`) -> rename / delete the mailbox itself
/// - `p` -> post to the mailbox's submission address
fn rights_from_myrights(rights: &MailboxRights) -> ContainerRights {
    let mailbox_admin =
        rights.contains(AclRight::DeleteMailbox) || rights.contains(AclRight::DeleteLegacy);
    ContainerRights {
        may_read_items: Some(rights.can_read()),
        may_add_items: Some(rights.contains(AclRight::Insert)),
        may_remove_items: Some(rights.contains(AclRight::DeleteMessages)),
        may_set_seen: Some(rights.contains(AclRight::Seen)),
        may_set_keywords: Some(rights.contains(AclRight::Write)),
        may_create_child: Some(
            rights.contains(AclRight::CreateMailbox) || rights.contains(AclRight::CreateLegacy),
        ),
        may_rename: Some(mailbox_admin),
        may_delete: Some(mailbox_admin),
        may_submit: Some(rights.contains(AclRight::Post)),
    }
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
        return MailboxName::new(leaf.to_owned()).map_err(|e| pim_malformed(e.to_string()));
    };
    let parent_folder = folder_from_container(parent)?;
    let delimiter = account
        .folders
        .get(&parent_folder)
        .and_then(|entry| entry.delimiter)
        .ok_or_else(|| super::error::unsupported(AccountOperation::ContainerCreate))?;
    MailboxName::new(format!("{}{delimiter}{leaf}", parent_folder.as_str()))
        .map_err(|e| pim_malformed(e.to_string()))
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
        return MailboxName::new(new_leaf.to_owned()).map_err(|e| pim_malformed(e.to_string()));
    };
    if let Some((parent, _)) = folder.as_str().rsplit_once(delimiter) {
        MailboxName::new(format!("{parent}{delimiter}{new_leaf}"))
            .map_err(|e| pim_malformed(e.to_string()))
    } else {
        MailboxName::new(new_leaf.to_owned()).map_err(|e| pim_malformed(e.to_string()))
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

async fn refresh_folders(account: &ImapAccount, op: AccountOperation) -> Result<(), AccountError> {
    let err = op_err(op);
    let conn = account.pool.dial_idle().await.map_err(err)?;
    let profile = conn.server_profile();
    let folders = factory::list_folders(&conn, &account.config, &profile)
        .await
        .map_err(err)?;
    // Personal-root re-LIST only. `replace_all` would clear the shared /
    // other-user entries NAMESPACE discovery installed at open, taking their
    // owner tags and MYRIGHTS with them, so every container mutation would
    // blank the shared half of `containers_list` until the next reopen.
    account.folders.replace_personal(folders);
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
    let flags = super::inventory::flags_set(fetch.flags.as_deref().unwrap_or(&[]));
    let importance = if flags.contains("$important") {
        Importance::High
    } else {
        Importance::Normal
    };
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
        importance,
        flags,
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

/// Serialize a `DraftPatch` into RFC 5322 octets via the shared
/// `bifrost-types` MIME assembler - the same path the send surface uses,
/// so IMAP has one composition path, not two. Inline attachments are now
/// supported on drafts; uploaded-attachment handles remain rejected (A6).
/// The `Bcc:` header is preserved in the saved draft body (unlike the
/// send path, which strips it).
fn draft_patch_to_rfc5322(patch: &DraftPatch) -> Result<Vec<u8>, AccountError> {
    if patch
        .attachments_uploaded
        .as_ref()
        .is_some_and(|attachments| !attachments.is_empty())
    {
        return Err(super::error::unsupported(
            AccountOperation::AttachmentUpload,
        ));
    }

    let from = patch.from.as_ref().and_then(Option::as_ref);
    let empty_addresses: Vec<Address> = Vec::new();
    let empty_strings: Vec<String> = Vec::new();
    let empty_attachments: Vec<bifrost_types::AttachmentInline> = Vec::new();

    let composed = bifrost_types::ComposedMessage {
        from,
        to: patch.to.as_deref().unwrap_or(&empty_addresses),
        cc: patch.cc.as_deref().unwrap_or(&empty_addresses),
        bcc: patch.bcc.as_deref().unwrap_or(&empty_addresses),
        reply_to: patch.reply_to.as_deref().unwrap_or(&empty_addresses),
        subject: patch.subject.as_ref().and_then(Option::as_deref),
        body_text: patch.body_text.as_ref().and_then(Option::as_deref),
        body_html: patch.body_html.as_ref().and_then(Option::as_deref),
        attachments_inline: patch
            .attachments_inline
            .as_deref()
            .unwrap_or(&empty_attachments),
        in_reply_to: patch.in_reply_to.as_ref().and_then(Option::as_deref),
        references: patch.references.as_deref().unwrap_or(&empty_strings),
        message_id: None,
        include_bcc_header: true,
        // The draft path carries no read-receipt request (that field lives
        // on SendRequest, not DraftPatch).
        disposition_notification_to: None,
    };
    Ok(bifrost_types::render_rfc5322(&composed))
}

/// Build a `Request(Malformed)` `AccountError` for local PIM failures
/// where the caller provided invalid or internally inconsistent state.
///
/// Operation is intentionally absent: `pim_malformed` is used from
/// helpers (decode_thread_id, mailbox-name validation, container-id
/// shape) that may be reached from multiple PIM ops. Each public PIM
/// surface threads its operation through `op_err` on wire errors;
/// these locally-malformed cases land in `ClientBug` regardless of op.
fn pim_malformed(detail: impl Into<String>) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail.into()),
        }),
    )
    .protocol(Protocol::Imap)
    .try_build()
    .expect("valid account error classification")
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::folder_registry::{FolderRegistry, SharedFolderEntry};
    use crate::types::MailboxInfo;

    fn registry_with_shared(rights: Option<&str>) -> FolderRegistry {
        let personal = MailboxInfo {
            name: MailboxName::new("INBOX").expect("valid mailbox"),
            delimiter: Some('/'),
            ..Default::default()
        };
        let shared = MailboxInfo {
            name: MailboxName::new("Shared/alice/Reports").expect("valid mailbox"),
            delimiter: Some('/'),
            ..Default::default()
        };
        FolderRegistry::from_lists(
            vec![personal],
            vec![SharedFolderEntry {
                info: shared,
                owner: bifrost_types::MailboxId("alice".to_owned()),
                rights: rights.map(MailboxRights::parse),
                namespace_prefix: "Shared/".to_owned(),
            }],
        )
    }

    #[test]
    fn container_projection_namespaces_shared_folder_and_leaves_personal_alone() {
        let registry = registry_with_shared(Some("lr"));

        let shared = registry
            .get(&MailboxName::new("Shared/alice/Reports").expect("valid mailbox"))
            .expect("shared entry");
        let container = container_from_folder_entry(&shared, "Reports".to_owned());
        assert_eq!(container.namespace, ContainerNamespace::Shared);
        assert_eq!(
            container.owner,
            Some(bifrost_types::MailboxId("alice".to_owned()))
        );
        // IMAP's owner-local id is the full path: it is already the native
        // id in the owner's namespace, and it matches the cursor-scope
        // string byte for byte.
        assert_eq!(
            container.owner_local_id.as_deref(),
            Some("Shared/alice/Reports")
        );
        assert_eq!(container.native_id, "Shared/alice/Reports");
        // No provider types IMAP folders, so `content_class` stays None.
        assert!(container.content_class.is_none());

        let personal = registry
            .get(&MailboxName::new("INBOX").expect("valid mailbox"))
            .expect("personal entry");
        let container = container_from_folder_entry(&personal, "INBOX".to_owned());
        assert_eq!(container.namespace, ContainerNamespace::Personal);
        assert!(container.owner.is_none());
        assert!(container.owner_local_id.is_none());
        // A personal folder was never MYRIGHTS-probed, so rights are
        // unreported rather than "no rights".
        assert!(container.rights.is_none());
    }

    // The rights projection has to survive the case where the personal
    // `LIST "" "*"` ALSO returned the shared path (RFC 2342 permits it and
    // several servers do it). Before the precedence fix the overlapping entry
    // stayed personal, so `Container::rights` came back `None` for a folder
    // whose MYRIGHTS had been parsed and then discarded.
    #[test]
    fn shared_container_keeps_its_rights_when_the_personal_list_overlaps() {
        let path = MailboxName::new("Shared/alice/Reports").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(
            vec![MailboxInfo {
                name: path.clone(),
                delimiter: Some('/'),
                ..Default::default()
            }],
            vec![SharedFolderEntry {
                info: MailboxInfo {
                    name: path.clone(),
                    delimiter: Some('/'),
                    ..Default::default()
                },
                owner: bifrost_types::MailboxId("alice".to_owned()),
                rights: Some(MailboxRights::parse("lr")),
                namespace_prefix: "Shared/".to_owned(),
            }],
        );

        let entry = registry.get(&path).expect("entry present");
        let container = container_from_folder_entry(&entry, "Reports".to_owned());
        assert_eq!(container.namespace, ContainerNamespace::Shared);
        let rights = container.rights.expect("MYRIGHTS reaches the container");
        // `lr` is a read-only share: readable, but no insert / delete / flag.
        assert_eq!(rights.may_read_items, Some(true));
        assert_eq!(rights.may_add_items, Some(false));
        assert_eq!(rights.may_remove_items, Some(false));
    }

    /// The container's `native_id` must be byte-identical to the
    /// `CursorScope::Folder` string discovery emits for the same folder -
    /// that identity is what lets the consumer join a container to its
    /// sync scope.
    #[test]
    fn container_native_id_matches_discovered_cursor_scope() {
        let registry = registry_with_shared(Some("lr"));
        for entry in registry.entries() {
            let scope = super::super::folder_scope(&entry.name);
            let container = container_from_folder_entry(&entry, "n/a".to_owned());
            assert_eq!(
                scope,
                bifrost_types::CursorScope::Folder(bifrost_types::FolderId(
                    container.native_id.clone()
                )),
                "container native_id must equal the emitted cursor scope string",
            );
        }
    }

    #[test]
    fn myrights_projection_distinguishes_read_only_from_writable() {
        // Read-only share: lookup + read only.
        let read_only = rights_from_myrights(&MailboxRights::parse("lr"));
        assert_eq!(read_only.may_read_items, Some(true));
        assert_eq!(read_only.may_add_items, Some(false));
        assert_eq!(read_only.may_remove_items, Some(false));
        assert_eq!(read_only.may_set_seen, Some(false));
        assert_eq!(read_only.may_set_keywords, Some(false));
        assert_eq!(read_only.may_create_child, Some(false));
        assert_eq!(read_only.may_rename, Some(false));
        assert_eq!(read_only.may_delete, Some(false));
        assert_eq!(read_only.may_submit, Some(false));

        // Full share.
        let full = rights_from_myrights(&MailboxRights::parse("lrswipkxtea"));
        assert_eq!(full.may_read_items, Some(true));
        assert_eq!(full.may_add_items, Some(true));
        assert_eq!(full.may_remove_items, Some(true));
        assert_eq!(full.may_set_seen, Some(true));
        assert_eq!(full.may_set_keywords, Some(true));
        assert_eq!(full.may_create_child, Some(true));
        assert_eq!(full.may_rename, Some(true));
        assert_eq!(full.may_delete, Some(true));
        assert_eq!(full.may_submit, Some(true));

        // The pre-4314 virtual rights map onto the same members.
        let legacy = rights_from_myrights(&MailboxRights::parse("lrcd"));
        assert_eq!(legacy.may_create_child, Some(true));
        assert_eq!(legacy.may_delete, Some(true));
    }

    #[test]
    fn importance_high_sets_keyword_others_clear() {
        // High -> set `$important`; Normal/Low -> clear it. One STORE op,
        // never an expand-into-two.
        assert!(importance_sets_important_keyword(Importance::High));
        assert!(!importance_sets_important_keyword(Importance::Normal));
        assert!(!importance_sets_important_keyword(Importance::Low));
    }

    #[test]
    fn send_as_rejected_unsupported() {
        let mut request = SendRequest::default();
        request.send_as = Some(bifrost_types::SendAs::As(bifrost_types::MailboxId(
            "shared@contoso.com".to_string(),
        )));
        let err = send_as_guard(&request).expect("send_as request must be rejected");
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
        assert_eq!(err.operation(), Some(AccountOperation::Send));

        // A personal send passes the guard.
        assert!(send_as_guard(&SendRequest::default()).is_none());
    }

    #[tokio::test]
    async fn host_attachment_unsupported() {
        // IMAP has no cloud-drive hosting; the leg returns
        // `Unsupported(HostAttachment)`.
        let err = unsupported_hosted(AccountOperation::HostAttachment)
            .await
            .expect_err("imap host_attachment is unsupported");
        assert_eq!(
            err.kind(),
            &bifrost_types::AccountErrorKind::Unsupported(AccountOperation::HostAttachment)
        );
    }

    #[test]
    fn scheduled_send_restamp_preserves_unsupported_kind() {
        // The smtp boundary already produces `Unsupported(Send)` for an
        // unsupporting relay (brick 4.6a). The IMAP send path re-stamps
        // only the operation/protocol via `restamp`; it must not change
        // the kind. Pin that the kind survives unchanged.
        let smtp_derived = bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send),
            bifrost_types::Cause::Request(bifrost_types::RequestCause::Unsupported {
                operation: AccountOperation::Send,
            }),
        )
        .protocol(Protocol::Smtp)
        .operation(AccountOperation::Send)
        .try_build()
        .expect("valid account error classification");

        let restamped = restamp(smtp_derived, AccountOperation::Send);
        assert_eq!(
            restamped.kind(),
            &bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send)
        );
        assert_eq!(restamped.operation(), Some(AccountOperation::Send));
    }

    #[test]
    fn draft_send_strips_bcc_into_envelope() {
        let raw = b"From: Me <me@sender.test>\r\n\
To: Ann <ann@to.test>\r\n\
Cc: cc@cc.test\r\n\
Bcc: blind1@bcc.test, Blind Two <blind2@bcc.test>\r\n\
Subject: Hi\r\n\
Message-ID: <draft-123@sender.test>\r\n\
\r\n\
body text\r\n";
        let default_from = bifrost_types::Address::bare("fallback@sender.test");
        let parsed = parse_draft_for_submission(raw, &default_from).expect("parses");

        // Bcc addresses are folded into the envelope recipients.
        let recipients: Vec<&str> = parsed
            .envelope
            .recipients
            .iter()
            .map(|a| a.address.as_str())
            .collect();
        assert_eq!(
            recipients,
            [
                "ann@to.test",
                "cc@cc.test",
                "blind1@bcc.test",
                "blind2@bcc.test"
            ]
        );
        assert_eq!(parsed.envelope.from.address, "me@sender.test");
        assert_eq!(parsed.message_id, "draft-123@sender.test");

        // Bcc header is stripped from the transmitted body; To/Cc remain.
        let body = String::from_utf8(parsed.body).expect("utf8");
        assert!(!body.to_ascii_lowercase().contains("bcc:"));
        assert!(body.contains("To: Ann <ann@to.test>"));
        assert!(body.contains("Cc: cc@cc.test"));
        assert!(body.contains("body text"));
    }

    #[test]
    fn draft_send_falls_back_to_default_from_and_requires_recipient() {
        let default_from = bifrost_types::Address::bare("fallback@sender.test");

        // No From header: reverse path falls back to default_from.
        let raw = b"To: ann@to.test\r\n\r\nhi\r\n";
        let parsed = parse_draft_for_submission(raw, &default_from).expect("parses");
        assert_eq!(parsed.envelope.from.address, "fallback@sender.test");

        // No recipients at all: malformed.
        let raw = b"From: me@sender.test\r\n\r\nhi\r\n";
        let err = parse_draft_for_submission(raw, &default_from).expect_err("no recipients");
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
    }

    #[test]
    fn draft_send_preserves_non_utf8_body_bytes_verbatim() {
        // An 8-bit / binary body octet (here 0xFF, invalid UTF-8) must
        // survive byte-exact in the transmitted body. The previous
        // `from_utf8_lossy` path turned it into U+FFFD (0xEF 0xBF 0xBD).
        let mut raw = b"From: me@sender.test\r\nTo: ann@to.test\r\n\r\n".to_vec();
        raw.extend_from_slice(&[0xFF, 0x00, 0xFE, b'\r', b'\n']);
        let default_from = bifrost_types::Address::bare("fallback@sender.test");
        let parsed = parse_draft_for_submission(&raw, &default_from).expect("parses");

        // Body retains the verbatim 8-bit octets; no U+FFFD substitution.
        assert!(parsed.body.ends_with(&[0xFF, 0x00, 0xFE, b'\r', b'\n']));
        assert!(!parsed.body.windows(3).any(|w| w == [0xEF, 0xBF, 0xBD]));
    }

    #[test]
    fn draft_send_bare_lf_split_and_strip_keeps_lf_endings() {
        // A bare-LF draft (no CRLF) must split on `\n\n` and the rebuilt
        // body must not gain a stray CRLF separator (no mixed endings).
        let raw = b"From: me@sender.test\nTo: ann@to.test\nBcc: blind@bcc.test\n\nbody line\n";
        let default_from = bifrost_types::Address::bare("fallback@sender.test");
        let parsed = parse_draft_for_submission(raw, &default_from).expect("parses");

        let body = parsed.body;
        // Bcc folded into recipients.
        assert!(
            parsed
                .envelope
                .recipients
                .iter()
                .any(|a| a.address == "blind@bcc.test")
        );
        // No CRLF anywhere: the bare-LF draft stays bare-LF.
        assert!(!body.windows(2).any(|w| w == b"\r\n"));
        // Bcc header line is gone; To survives.
        let text = String::from_utf8(body).expect("ascii");
        assert!(!text.to_ascii_lowercase().contains("bcc:"));
        assert!(text.contains("To: ann@to.test"));
        assert!(text.contains("body line"));
    }

    #[test]
    fn draft_send_header_only_draft_has_empty_body() {
        // A header-only draft (no blank line, no body) is degenerate but
        // recoverable: the split yields an empty body and the envelope is
        // still built from the headers.
        let raw = b"From: me@sender.test\r\nTo: ann@to.test\r\n";
        let default_from = bifrost_types::Address::bare("fallback@sender.test");
        let parsed = parse_draft_for_submission(raw, &default_from).expect("parses");
        assert_eq!(parsed.envelope.recipients[0].address, "ann@to.test");
        // Header block ends with CRLF + a synthesized blank line, then no body.
        let text = String::from_utf8(parsed.body).expect("ascii");
        assert!(text.contains("To: ann@to.test"));
        assert!(text.ends_with("\r\n\r\n"));
    }

    #[test]
    fn split_header_body_handles_mixed_and_header_only() {
        // CRLF blank line: header_end after `A: 1\r\nB: 2\r\n` (12),
        // body_start after the second `\r\n` (14).
        assert_eq!(split_header_body(b"A: 1\r\nB: 2\r\n\r\nbody"), (12, 14));
        // Bare-LF blank line: header_end after `A: 1\nB: 2\n` (10),
        // body_start after the second `\n` (11).
        assert_eq!(split_header_body(b"A: 1\nB: 2\n\nbody"), (10, 11));
        // No blank line: header-only.
        let raw = b"A: 1\r\n";
        assert_eq!(split_header_body(raw), (raw.len(), raw.len()));
    }

    #[test]
    fn or_with_nested_in_folder_is_rejected() {
        // An `In(folder)` nested inside an `Or` cannot be expressed as a
        // single-mailbox IMAP SEARCH; reject rather than silently leak
        // the folder restriction to the whole OR group.
        use bifrost_types::search::SearchFilter;
        let filter = SearchFilter::Or(vec![
            SearchFilter::In(ContainerId("Archive".to_owned())),
            SearchFilter::From("a@b.test".to_owned()),
        ]);
        let err = match criteria_from_filter(&filter) {
            Err(err) => err,
            Ok(_) => panic!("OR with In must be rejected"),
        };
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        );

        // An `In` inside an `And` is still accepted (the folder narrows
        // the whole conjunction - defensible).
        let filter = SearchFilter::And(vec![
            SearchFilter::In(ContainerId("Archive".to_owned())),
            SearchFilter::From("a@b.test".to_owned()),
        ]);
        let part = match criteria_from_filter(&filter) {
            Ok(part) => part,
            Err(_) => panic!("AND with In is accepted"),
        };
        assert_eq!(
            part.folder.as_ref().map(MailboxName::as_str),
            Some("Archive")
        );
    }

    #[test]
    fn folder_role_prefers_special_use_over_the_folder_name() {
        // A localized mailbox is only recognizable by its SPECIAL-USE
        // attribute, and a mislabelled name must not beat the attribute.
        assert_eq!(
            folder_role(&[MailboxAttribute::Sent], "Gesendete Objekte"),
            Some(FolderRole::Sent)
        );
        assert_eq!(
            folder_role(&[MailboxAttribute::Junk], "Archive"),
            Some(FolderRole::Spam),
            "the attribute wins over the name",
        );
        assert_eq!(
            folder_role(
                &[MailboxAttribute::Custom("\\INBOX".to_owned())],
                "Posteingang"
            ),
            Some(FolderRole::Inbox),
            "the \\Inbox custom attribute is matched case-insensitively",
        );
    }

    #[test]
    fn folder_role_falls_back_to_the_slash_delimited_leaf_name() {
        assert_eq!(folder_role(&[], "INBOX"), Some(FolderRole::Inbox));
        assert_eq!(folder_role(&[], "inbox"), Some(FolderRole::Inbox));
        assert_eq!(
            folder_role(&[], "[Gmail]/Sent Mail"),
            Some(FolderRole::Sent)
        );
        assert_eq!(
            folder_role(&[], "Deleted Messages"),
            Some(FolderRole::Trash)
        );
        assert_eq!(folder_role(&[], "Projects/Q3"), None);
        assert_eq!(
            folder_role(&[MailboxAttribute::HasChildren], "Whatever"),
            None,
            "a non-special-use attribute contributes no role",
        );
    }

    // NOTE: this pins CURRENT behavior, which is believed WRONG. The
    // leaf-name fallback splits on `/` only, but the hierarchy delimiter
    // is per-server (Courier and several Dovecot layouts use `.`), and
    // the real delimiter is already known - it rides on `FolderEntry`.
    // On such a server no role resolves, so `role_folder(Sent)` is None
    // and the Sent-copy APPEND, `draft_create` and `delete_thread`
    // silently lose their targets.
    #[test]
    fn folder_role_misses_the_leaf_when_the_delimiter_is_not_a_slash() {
        assert_eq!(folder_role(&[], "INBOX.Sent"), None);
        assert_eq!(folder_role(&[], "INBOX.Trash"), None);
        // The same layout with `/` resolves, which is the asymmetry.
        assert_eq!(folder_role(&[], "INBOX/Sent"), Some(FolderRole::Sent));
    }

    #[test]
    fn parent_id_needs_the_servers_delimiter() {
        assert_eq!(
            parent_id(Some('/'), "Projects/Q3"),
            Some(ContainerId("Projects".to_owned()))
        );
        assert_eq!(
            parent_id(Some('.'), "INBOX.Projects.Q3"),
            Some(ContainerId("INBOX.Projects".to_owned())),
            "the delimiter is taken from LIST, not assumed",
        );
        assert_eq!(parent_id(Some('/'), "INBOX"), None, "a root has no parent");
        assert_eq!(
            parent_id(None, "Projects/Q3"),
            None,
            "a flat server has no hierarchy at all",
        );
    }

    #[test]
    fn convenience_keywords_map_onto_system_flags() {
        assert_eq!(imap_flag_for_keyword("$flagged"), Flag::Flagged);
        assert_eq!(imap_flag_for_keyword("$FLAGGED"), Flag::Flagged);
        assert_eq!(imap_flag_for_keyword("\\Flagged"), Flag::Flagged);
        assert_eq!(imap_flag_for_keyword("$answered"), Flag::Answered);
        assert_eq!(imap_flag_for_keyword("$seen"), Flag::Seen);
        // Everything else stays a keyword, verbatim: `$forwarded` and
        // `$MDNSent` have no system-flag equivalent.
        assert_eq!(
            imap_flag_for_keyword("$MDNSent"),
            Flag::Custom("$MDNSent".to_owned())
        );
        assert_eq!(
            imap_flag_for_keyword("$important"),
            Flag::Custom("$important".to_owned())
        );
    }

    fn search_request(limit: Option<u32>, cursor: Option<&str>) -> SearchRequest {
        let mut request = SearchRequest::default();
        request.limit = limit;
        request.page_cursor = cursor.map(|c| c.as_bytes().to_vec());
        request
    }

    #[test]
    fn page_from_items_walks_an_offset_cursor_to_exhaustion() {
        let items: Vec<u32> = (0..5).collect();

        let first = page_from_items(items.clone(), &search_request(Some(2), None)).expect("page");
        assert_eq!(first.items, vec![0, 1]);
        assert_eq!(first.next_cursor.as_deref(), Some(b"2".as_slice()));
        assert_eq!(first.estimated_total, Some(5));

        let last =
            page_from_items(items.clone(), &search_request(Some(2), Some("4"))).expect("page");
        assert_eq!(last.items, vec![4]);
        assert_eq!(last.next_cursor, None, "the final page ends the walk");

        // A cursor at or past the end yields an empty final page rather
        // than an error or a panic on the slice.
        let past = page_from_items(items, &search_request(Some(2), Some("99"))).expect("page");
        assert!(past.items.is_empty());
        assert_eq!(past.next_cursor, None);
    }

    #[test]
    fn page_from_items_rejects_a_cursor_it_did_not_mint() {
        let err = page_from_items(vec![1u32], &search_request(None, Some("not-a-number")))
            .expect_err("garbage cursor");
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
    }

    #[test]
    fn imap_date_uses_the_rfc3501_date_text_production() {
        // date-text = date-day "-" date-month "-" date-year, and
        // date-day is 1*2DIGIT, so a single-digit day is not zero-padded.
        assert_eq!(imap_date(SystemTime::UNIX_EPOCH), "1-Jan-1970");
        assert_eq!(
            imap_date(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(60 * 60 * 24 * 364)),
            "31-Dec-1970"
        );
    }

    #[test]
    fn search_plan_defaults_to_all_and_carries_the_folder_restriction() {
        let plan = search_plan(&SearchRequest::default()).expect("plan");
        assert_eq!(plan.criteria, "ALL");
        assert!(plan.folder.is_none());

        let plan = search_plan(&SearchRequest::filter(SearchFilter::In(ContainerId(
            "Archive".to_owned(),
        ))))
        .expect("plan");
        assert_eq!(plan.criteria, "ALL", "In contributes no criteria");
        assert_eq!(
            plan.folder.as_ref().map(MailboxName::as_str),
            Some("Archive")
        );
    }

    #[test]
    fn search_plan_quotes_filter_operands() {
        let plan = search_plan(&SearchRequest::filter(SearchFilter::Subject(
            "quarterly \"report\"".to_owned(),
        )))
        .expect("plan");
        assert_eq!(plan.criteria, "SUBJECT \"quarterly \\\"report\\\"\"");
    }

    #[test]
    fn search_plan_substitutes_or_conjoins_the_provider_query() {
        // With no structured filter the raw query replaces the ALL.
        let plan = search_plan(&SearchRequest::provider("  UNSEEN  ")).expect("plan");
        assert_eq!(plan.criteria, "UNSEEN", "the raw query is trimmed");

        // With a structured filter the two are juxtaposed, which is
        // IMAP's implicit AND (RFC 3501 Section 6.4.4).
        let mut request = SearchRequest::filter(SearchFilter::From("a@b.test".to_owned()));
        request.provider_query = Some("UNSEEN".to_owned());
        let plan = search_plan(&request).expect("plan");
        assert_eq!(plan.criteria, "FROM \"a@b.test\" UNSEEN");

        // A whitespace-only raw query is ignored entirely.
        let plan = search_plan(&SearchRequest::provider("   ")).expect("plan");
        assert_eq!(plan.criteria, "ALL");
    }

    // NOTE: this pins CURRENT behavior, which is believed WRONG.
    // `provider_query` is spliced into the criteria string with no
    // syntactic validation whatsoever, so an unbalanced `)` reaches the
    // connection layer's SEARCH-criteria scanner - where it currently
    // spins forever.
    #[test]
    fn search_plan_passes_an_unbalanced_provider_query_straight_through() {
        let plan = search_plan(&SearchRequest::provider(")")).expect("plan");
        assert_eq!(plan.criteria, ")", "no parenthesis balancing is applied");

        let plan = search_plan(&SearchRequest::provider("FROM")).expect("plan");
        assert_eq!(plan.criteria, "FROM", "no operand-arity check is applied");
    }

    #[test]
    fn and_and_or_combinators_produce_the_expected_criteria_shapes() {
        let and = combine_and(&[
            SearchFilter::From("a@b.test".to_owned()),
            SearchFilter::Subject("hi".to_owned()),
        ])
        .expect("and");
        assert_eq!(and.criteria, "FROM \"a@b.test\" SUBJECT \"hi\"");

        // An `In`-only conjunction degenerates to ALL plus the folder.
        let folder_only =
            combine_and(&[SearchFilter::In(ContainerId("Archive".to_owned()))]).expect("and");
        assert_eq!(folder_only.criteria, "ALL");
        assert_eq!(
            folder_only.folder.as_ref().map(MailboxName::as_str),
            Some("Archive")
        );

        // RFC 3501 OR takes exactly two search-keys, so a three-way OR
        // has to nest.
        let or = combine_or(&[
            SearchFilter::From("a@b.test".to_owned()),
            SearchFilter::From("c@d.test".to_owned()),
            SearchFilter::From("e@f.test".to_owned()),
        ])
        .expect("or");
        assert_eq!(
            or.criteria,
            "OR (OR (FROM \"a@b.test\") (FROM \"c@d.test\")) (FROM \"e@f.test\")"
        );

        assert_eq!(combine_or(&[]).expect("empty or").criteria, "ALL");
    }

    #[test]
    fn and_across_two_folders_is_rejected_rather_than_silently_narrowed() {
        // IMAP SEARCH runs against one selected mailbox, so a filter
        // naming two folders has no faithful single-command encoding.
        let err = combine_and(&[
            SearchFilter::In(ContainerId("Archive".to_owned())),
            SearchFilter::In(ContainerId("Sent".to_owned())),
        ])
        .expect_err("two folders");
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
        // The same folder twice is not a conflict.
        assert!(
            combine_and(&[
                SearchFilter::In(ContainerId("Archive".to_owned())),
                SearchFilter::In(ContainerId("Archive".to_owned())),
            ])
            .is_ok()
        );
    }

    #[test]
    fn not_wraps_its_inner_criteria_and_keeps_the_folder() {
        let part = criteria_from_filter(&SearchFilter::Not(Box::new(SearchFilter::And(vec![
            SearchFilter::In(ContainerId("Archive".to_owned())),
            SearchFilter::From("a@b.test".to_owned()),
        ]))))
        .expect("not");
        assert_eq!(part.criteria, "NOT (FROM \"a@b.test\")");
        assert_eq!(
            part.folder.as_ref().map(MailboxName::as_str),
            Some("Archive")
        );
    }

    #[test]
    fn header_name_matching_is_trimmed_and_case_insensitive() {
        assert!(header_name_is_bcc(b"Bcc: x@y"));
        assert!(header_name_is_bcc(b"BCC:x@y"));
        assert!(header_name_is_bcc(b" bcc : x@y"));
        assert!(!header_name_is_bcc(b"Bcc-Original: x@y"));
        assert!(!header_name_is_bcc(b"X-Original-Bcc: x@y"));
        assert!(!header_name_is_bcc(b"Bcc x@y"), "no colon, no header");
    }

    #[test]
    fn strip_bcc_drops_the_folded_continuation_lines_too() {
        // A folded `Bcc:` whose continuation lines were kept would leak
        // the blind recipients as a dangling header fragment.
        let header = b"To: a@b.test\r\nBcc: x@y.test,\r\n\tz@w.test\r\nSubject: hi\r\n";
        let out = strip_bcc_header(header, b"body");
        let text = String::from_utf8(out).expect("ascii");
        assert!(!text.to_ascii_lowercase().contains("bcc"));
        assert!(
            !text.contains("z@w.test"),
            "the fold must go with its header"
        );
        assert!(text.contains("To: a@b.test"));
        assert!(text.contains("Subject: hi"));
        assert!(text.ends_with("\r\n\r\nbody"));
    }

    #[test]
    fn unfold_headers_joins_folds_and_keeps_repeats_in_order() {
        let headers = unfold_headers("To: a@b.test,\r\n  c@d.test\r\nTo: e@f.test\r\nX: 1\r\n");
        assert_eq!(
            headers,
            vec![
                ("to".to_owned(), "a@b.test, c@d.test".to_owned()),
                ("to".to_owned(), "e@f.test".to_owned()),
                ("x".to_owned(), "1".to_owned()),
            ]
        );
        // Both `To:` headers contribute recipients.
        let addrs: Vec<String> = addresses_for(&headers, "to")
            .into_iter()
            .map(|a| a.address)
            .collect();
        assert_eq!(addrs, ["a@b.test", "c@d.test", "e@f.test"]);
    }

    #[test]
    fn address_list_splitting_ignores_commas_inside_angle_brackets() {
        let items = split_address_list("<a@b, c>, d@e");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].trim(), "<a@b, c>");
        assert_eq!(items[1].trim(), "d@e");
    }

    #[test]
    fn hydration_attrs_track_the_projection() {
        for projection in [
            HydrationProjection::Headers,
            HydrationProjection::Preview(64),
            HydrationProjection::Full,
            HydrationProjection::FullWithBlobs,
        ] {
            let attrs = attrs_for_hydration(projection);
            assert!(attrs.iter().any(|attr| matches!(attr, FetchAttr::Envelope)));
            assert!(attrs.iter().any(|attr| matches!(attr, FetchAttr::Uid)));
        }

        assert!(
            attrs_for_hydration(HydrationProjection::Headers)
                .iter()
                .all(|attr| !matches!(attr, FetchAttr::BodySection { .. })),
            "a headers-only hydration must not pull a body",
        );

        let preview = attrs_for_hydration(HydrationProjection::Preview(64));
        assert!(preview.iter().any(|attr| matches!(
            attr,
            FetchAttr::BodySection {
                peek: true,
                section: Some(s),
                partial: Some((0, 64)),
            } if s == "TEXT"
        )));

        let full = attrs_for_hydration(HydrationProjection::Full);
        assert!(full.iter().any(|attr| matches!(
            attr,
            FetchAttr::BodySection {
                peek: true,
                section: None,
                partial: None,
            }
        )));
    }

    #[test]
    fn hydrated_message_maps_importance_from_the_important_keyword() {
        let with = crate::types::FetchResponse {
            uid: Some(2),
            flags: Some(vec![Flag::Custom("$Important".to_owned())]),
            ..Default::default()
        };
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let message =
            fetch_to_message(&folder, 9, with, HydrationProjection::Headers).expect("uid present");
        assert_eq!(message.importance, Importance::High);
        assert_eq!(message.containers, vec![ContainerId("INBOX".to_owned())]);
        assert!(
            message.body_text.is_none(),
            "a headers projection carries no body"
        );

        let without = crate::types::FetchResponse {
            uid: Some(2),
            ..Default::default()
        };
        let message = fetch_to_message(&folder, 9, without, HydrationProjection::Headers)
            .expect("uid present");
        assert_eq!(message.importance, Importance::Normal);
    }

    // NOTE: this pins CURRENT behavior, which is believed WRONG. A `Full`
    // hydration fetches `BODY.PEEK[]` - the ENTIRE RFC 5322 message - and
    // assigns the lossy-UTF-8 string of it to `Message::body_text`, so the
    // consumer receives headers and MIME boundaries in a field documented
    // as the text body.
    #[test]
    fn full_hydration_puts_the_whole_raw_message_in_body_text() {
        let fetch = crate::types::FetchResponse {
            uid: Some(2),
            body_sections: vec![crate::types::fetch::BodySection {
                section: String::new(),
                origin: None,
                data: Some(b"Subject: hi\r\n\r\nthe body".to_vec()),
            }],
            ..Default::default()
        };
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let message =
            fetch_to_message(&folder, 9, fetch, HydrationProjection::Full).expect("uid present");
        let body = message.body_text.expect("full projection carries a body");
        assert!(
            body.starts_with("Subject: hi"),
            "documents the un-parsed raw message; not an endorsement",
        );
        assert!(message.body_html.is_none());
        assert!(message.attachments.is_empty());
    }

    #[test]
    fn parse_address_list_respects_quoted_commas() {
        // A display name this crate's own assembler quotes (`"Last, First"`)
        // must not split into a phantom recipient.
        let parsed = parse_address_list("\"Doe, Jane\" <jane@to.test>, bob@to.test");
        let addrs: Vec<&str> = parsed.iter().map(|a| a.address.as_str()).collect();
        assert_eq!(addrs, ["jane@to.test", "bob@to.test"]);
        assert_eq!(parsed[0].name.as_deref(), Some("Doe, Jane"));
    }
}
