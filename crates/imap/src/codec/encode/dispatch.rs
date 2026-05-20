use bytes::BytesMut;

use crate::types::Command;
use crate::types::response::Capability;

use super::commands::{
    encode_authenticate, encode_create_special_use, encode_fetch, encode_getmetadata, encode_id,
    encode_list_extended, encode_list_status, encode_login, encode_mailbox_cmd,
    encode_mailbox_name, encode_mailbox_str, encode_notify_set, encode_search,
    encode_select_or_examine, encode_set_acl, encode_set_quota, encode_setmetadata, encode_simple,
    encode_status, encode_store, encode_thread_or_sort_cmd, encode_two_arg, encode_two_quoted_args,
    encode_uid_expunge, encode_uid_fetch,
};
use super::{EncodeError, EncodeOptions, EncodedCommand, encode_enable, validate_atom};

/// Encodes an IMAP command into an [`EncodedCommand`] split at synchronizing
/// literal boundaries (RFC 3501 Section 4.3).
///
/// The `literal_mode` parameter controls literal marker style:
/// - [`super::LiteralMode::LiteralPlus`] (RFC 7888 Section 4):
///   non-synchronizing for all sizes  -  the result is a single segment.
/// - [`super::LiteralMode::LiteralMinus`] (RFC 7888 Section 5):
///   non-synchronizing for literals <= 4096 bytes; larger literals use
///   synchronizing form and produce segment splits.
/// - [`super::LiteralMode::Synchronizing`] (RFC 3501 Section 4.3):
///   all literals are synchronizing  -  segments split at each literal boundary.
///
/// When `opts.utf8_mode` is `true` (UTF8=ACCEPT is active per RFC 6855
/// Section 3), non-ASCII UTF-8 bytes are permitted in quoted strings.
/// RFC 6855 Section 3: "The server MUST accept UTF-8 in quoted strings."
/// RFC 9051 Section 9 extends CHAR to include UTF8-2/UTF8-3/UTF8-4.
///
/// Capability prerequisite checks (I6) are performed before any bytes are
/// produced: commands that require capabilities not present in
/// `opts.capabilities` / `opts.enabled` return `EncodeError::MissingCapability`.
///
/// APPEND is handled separately by `ImapConnection::append()`.
/// Tag-command format per RFC 3501 Section 2.2.1 / RFC 9051 Section 2.2.1.
#[allow(clippy::too_many_lines)]
pub(crate) fn encode_command(
    tag: &str,
    command: &Command,
    opts: &EncodeOptions,
) -> Result<EncodedCommand, EncodeError> {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, tag, command, opts)?;
    Ok(EncodedCommand::from_flat_buffer(&buf))
}

/// Encodes an IMAP command into the provided buffer as a flat byte sequence.
///
/// This is the internal encoding engine. Callers that need synchronizing-literal
/// awareness should use [`encode_command`] which wraps this and returns an
/// [`EncodedCommand`] with properly split segments.
///
/// Capability prerequisite checks (I6) run before bytes are produced.
///
/// Tag-command format per RFC 3501 Section 2.2.1 / RFC 9051 Section 2.2.1.
#[allow(clippy::too_many_lines)]
pub(super) fn encode_command_to_buf(
    buf: &mut BytesMut,
    tag: &str,
    command: &Command,
    opts: &EncodeOptions,
) -> Result<(), EncodeError> {
    let literal_mode = opts.literal_mode;
    let utf8 = opts.utf8_mode;
    match command {
        Command::Login { user, pass } => {
            encode_login(buf, tag, user, pass, utf8, literal_mode)?;
        }
        Command::Authenticate {
            mechanism,
            initial_response,
        } => {
            // RFC 3501 Section 6.2.2: auth-type = atom
            validate_atom(mechanism, "AUTHENTICATE mechanism")?;
            encode_authenticate(buf, tag, mechanism, initial_response.as_deref())?;
        }
        // RFC 3501 Section 6.2.1 / RFC 9051 Section 6.2.1
        Command::StartTls => {
            if !opts.has_capability(&Capability::StartTls) {
                return Err(EncodeError::MissingCapability {
                    cmd: "STARTTLS",
                    cap: "STARTTLS".into(),
                });
            }
            encode_simple(buf, tag, "STARTTLS");
        }
        // RFC 3501 Section 6.1.3 / RFC 9051 Section 6.1.3
        Command::Logout => {
            encode_simple(buf, tag, "LOGOUT");
        }
        // RFC 5161 Section 3
        Command::Enable { capabilities } => {
            // RFC 5161 Section 3.1: ENABLE requires ENABLE capability.
            if !opts.has_capability(&Capability::Enable) {
                return Err(EncodeError::MissingCapability {
                    cmd: "ENABLE",
                    cap: "ENABLE".into(),
                });
            }
            encode_enable(buf, tag, capabilities)?;
        }
        Command::List { reference, pattern } => {
            let wire_ref = encode_mailbox_str(reference, utf8);
            let wire_pat = encode_mailbox_str(pattern, utf8);
            encode_two_quoted_args(buf, tag, "LIST", &wire_ref, &wire_pat, utf8, literal_mode);
        }
        Command::ListExtended {
            selection_options,
            reference,
            patterns,
            return_options,
        } => {
            encode_list_extended(
                buf,
                tag,
                selection_options,
                reference,
                patterns,
                return_options,
                utf8,
                literal_mode,
            )?;
        }
        Command::ListStatus {
            reference,
            pattern,
            status_items,
        } => {
            encode_list_status(
                buf,
                tag,
                reference,
                pattern,
                status_items,
                utf8,
                literal_mode,
            )?;
        }
        Command::Select {
            mailbox,
            condstore,
            qresync,
        } => {
            // RFC 7162 Section 3.1.1: CONDSTORE requires CONDSTORE capability.
            if *condstore && !opts.has_condstore() {
                return Err(EncodeError::MissingCapability {
                    cmd: "SELECT (CONDSTORE)",
                    cap: "CONDSTORE".into(),
                });
            }
            // RFC 7162 Section 3.2.5.2: QRESYNC requires QRESYNC capability.
            if qresync.is_some() && !opts.has_capability(&Capability::QResync) {
                return Err(EncodeError::MissingCapability {
                    cmd: "SELECT (QRESYNC)",
                    cap: "QRESYNC".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_select_or_examine(
                buf,
                tag,
                "SELECT",
                &wire,
                *condstore,
                qresync.as_ref(),
                utf8,
                literal_mode,
            )?;
        }
        Command::Examine {
            mailbox,
            condstore,
            qresync,
        } => {
            // RFC 7162 Section 3.1.1: CONDSTORE requires CONDSTORE capability.
            if *condstore && !opts.has_condstore() {
                return Err(EncodeError::MissingCapability {
                    cmd: "EXAMINE (CONDSTORE)",
                    cap: "CONDSTORE".into(),
                });
            }
            // RFC 7162 Section 3.2.5.2: QRESYNC requires QRESYNC capability.
            if qresync.is_some() && !opts.has_capability(&Capability::QResync) {
                return Err(EncodeError::MissingCapability {
                    cmd: "EXAMINE (QRESYNC)",
                    cap: "QRESYNC".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_select_or_examine(
                buf,
                tag,
                "EXAMINE",
                &wire,
                *condstore,
                qresync.as_ref(),
                utf8,
                literal_mode,
            )?;
        }
        Command::Create { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "CREATE", &wire, utf8, literal_mode);
        }
        Command::CreateSpecialUse {
            mailbox,
            special_use,
        } => {
            // RFC 6154 Section 3: CREATE-SPECIAL-USE capability required.
            if !opts.has_capability(&Capability::CreateSpecialUse) {
                return Err(EncodeError::MissingCapability {
                    cmd: "CREATE (USE)",
                    cap: "CREATE-SPECIAL-USE".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_create_special_use(buf, tag, &wire, special_use, utf8, literal_mode);
        }
        Command::Delete { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "DELETE", &wire, utf8, literal_mode);
        }
        Command::Rename { mailbox, new_name } => {
            let wire_old = encode_mailbox_name(mailbox, utf8);
            let wire_new = encode_mailbox_name(new_name, utf8);
            encode_two_quoted_args(buf, tag, "RENAME", &wire_old, &wire_new, utf8, literal_mode);
        }
        Command::Subscribe { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "SUBSCRIBE", &wire, utf8, literal_mode);
        }
        Command::Unsubscribe { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "UNSUBSCRIBE", &wire, utf8, literal_mode);
        }
        Command::Lsub { reference, pattern } => {
            let wire_ref = encode_mailbox_str(reference, utf8);
            let wire_pat = encode_mailbox_str(pattern, utf8);
            encode_two_quoted_args(buf, tag, "LSUB", &wire_ref, &wire_pat, utf8, literal_mode);
        }
        // RFC 3501 Section 6.4.2 / RFC 9051 Section 6.4.1
        Command::Close => {
            encode_simple(buf, tag, "CLOSE");
        }
        // RFC 8437 Section 2
        Command::Unauthenticate => {
            encode_simple(buf, tag, "UNAUTHENTICATE");
        }
        // RFC 3691 Section 3
        Command::Unselect => {
            // RFC 3691 Section 2: UNSELECT requires UNSELECT capability.
            if !opts.has_capability(&Capability::Unselect) {
                return Err(EncodeError::MissingCapability {
                    cmd: "UNSELECT",
                    cap: "UNSELECT".into(),
                });
            }
            encode_simple(buf, tag, "UNSELECT");
        }
        Command::Status { mailbox, items } => {
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_status(buf, tag, &wire, items, utf8, literal_mode)?;
        }
        Command::Search { criteria } => {
            encode_search(buf, tag, "SEARCH", criteria, None, utf8, literal_mode)?;
        }
        Command::SearchReturn {
            criteria,
            return_opts,
        } => {
            encode_search(
                buf,
                tag,
                "SEARCH",
                criteria,
                Some(return_opts),
                utf8,
                literal_mode,
            )?;
        }
        Command::SearchSave { criteria } => {
            encode_search(
                buf,
                tag,
                "SEARCH RETURN (SAVE)",
                criteria,
                None,
                utf8,
                literal_mode,
            )?;
        }
        Command::Fetch {
            sequence_set,
            items,
            changed_since,
        } => {
            // RFC 7162 Section 3.1.4.1: CHANGEDSINCE requires CONDSTORE.
            if changed_since.is_some() && !opts.has_condstore() {
                return Err(EncodeError::MissingCapability {
                    cmd: "FETCH (CHANGEDSINCE)",
                    cap: "CONDSTORE".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_fetch(buf, tag, sequence_set.as_str(), items, *changed_since)?;
        }
        Command::Store {
            sequence_set,
            operation,
            flags,
            unchanged_since,
        } => {
            // RFC 7162 Section 3.1.3: UNCHANGEDSINCE requires CONDSTORE.
            if unchanged_since.is_some() && !opts.has_condstore() {
                return Err(EncodeError::MissingCapability {
                    cmd: "STORE (UNCHANGEDSINCE)",
                    cap: "CONDSTORE".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_store(
                buf,
                tag,
                false,
                sequence_set.as_str(),
                *operation,
                flags,
                *unchanged_since,
            )?;
        }
        Command::Copy {
            sequence_set,
            mailbox,
        } => {
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_two_arg(
                buf,
                tag,
                false,
                "COPY",
                sequence_set.as_str(),
                &wire,
                utf8,
                literal_mode,
            )?;
        }
        Command::Move {
            sequence_set,
            mailbox,
        } => {
            // RFC 6851 Section 3: MOVE requires MOVE capability.
            if !opts.has_capability(&Capability::Move) {
                return Err(EncodeError::MissingCapability {
                    cmd: "MOVE",
                    cap: "MOVE".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_two_arg(
                buf,
                tag,
                false,
                "MOVE",
                sequence_set.as_str(),
                &wire,
                utf8,
                literal_mode,
            )?;
        }
        Command::UidSearch { criteria } => {
            encode_search(buf, tag, "UID SEARCH", criteria, None, utf8, literal_mode)?;
        }
        Command::UidSearchReturn {
            criteria,
            return_opts,
        } => {
            encode_search(
                buf,
                tag,
                "UID SEARCH",
                criteria,
                Some(return_opts),
                utf8,
                literal_mode,
            )?;
        }
        Command::UidSearchSave { criteria } => {
            encode_search(
                buf,
                tag,
                "UID SEARCH RETURN (SAVE)",
                criteria,
                None,
                utf8,
                literal_mode,
            )?;
        }
        Command::UidFetch {
            sequence_set,
            items,
            changed_since,
            vanished,
        } => {
            // RFC 7162 Section 3.1.4.1: CHANGEDSINCE requires CONDSTORE.
            if changed_since.is_some() && !opts.has_condstore() {
                return Err(EncodeError::MissingCapability {
                    cmd: "UID FETCH (CHANGEDSINCE)",
                    cap: "CONDSTORE".into(),
                });
            }
            // RFC 7162 Section 3.2.6: VANISHED requires QRESYNC.
            if *vanished && !opts.has_capability(&Capability::QResync) {
                return Err(EncodeError::MissingCapability {
                    cmd: "UID FETCH (VANISHED)",
                    cap: "QRESYNC".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_uid_fetch(
                buf,
                tag,
                sequence_set.as_str(),
                items,
                *changed_since,
                *vanished,
            )?;
        }
        Command::UidStore {
            sequence_set,
            operation,
            flags,
            unchanged_since,
        } => {
            // RFC 7162 Section 3.1.3: UNCHANGEDSINCE requires CONDSTORE.
            if unchanged_since.is_some() && !opts.has_condstore() {
                return Err(EncodeError::MissingCapability {
                    cmd: "UID STORE (UNCHANGEDSINCE)",
                    cap: "CONDSTORE".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_store(
                buf,
                tag,
                true,
                sequence_set.as_str(),
                *operation,
                flags,
                *unchanged_since,
            )?;
        }
        Command::UidMove {
            sequence_set,
            mailbox,
        } => {
            // RFC 6851 Section 3: MOVE requires MOVE capability.
            if !opts.has_capability(&Capability::Move) {
                return Err(EncodeError::MissingCapability {
                    cmd: "UID MOVE",
                    cap: "MOVE".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_two_arg(
                buf,
                tag,
                true,
                "MOVE",
                sequence_set.as_str(),
                &wire,
                utf8,
                literal_mode,
            )?;
        }
        Command::UidCopy {
            sequence_set,
            mailbox,
        } => {
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_two_arg(
                buf,
                tag,
                true,
                "COPY",
                sequence_set.as_str(),
                &wire,
                utf8,
                literal_mode,
            )?;
        }
        Command::UidExpunge { sequence_set } => {
            // RFC 4315: UID EXPUNGE requires UIDPLUS capability.
            if !opts.has_capability(&Capability::UidPlus) {
                return Err(EncodeError::MissingCapability {
                    cmd: "UID EXPUNGE",
                    cap: "UIDPLUS".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_uid_expunge(buf, tag, sequence_set.as_str())?;
        }
        // RFC 2342 Section 5 / RFC 9051 Section 6.3.10
        Command::Namespace => {
            if !opts.has_capability(&Capability::Namespace) {
                return Err(EncodeError::MissingCapability {
                    cmd: "NAMESPACE",
                    cap: "NAMESPACE".into(),
                });
            }
            encode_simple(buf, tag, "NAMESPACE");
        }
        // RFC 3501 Section 6.4.1 (removed in IMAP4rev2)
        Command::Check => {
            encode_simple(buf, tag, "CHECK");
        }
        // RFC 3501 Section 6.4.3 / RFC 9051 Section 6.4.3
        Command::Expunge => {
            encode_simple(buf, tag, "EXPUNGE");
        }
        // RFC 2177 Section 3 / RFC 9051 Section 6.3.13
        Command::Idle => {
            // RFC 2177 Section 1: IDLE requires IDLE capability.
            if !opts.has_capability(&Capability::Idle) {
                return Err(EncodeError::MissingCapability {
                    cmd: "IDLE",
                    cap: "IDLE".into(),
                });
            }
            encode_simple(buf, tag, "IDLE");
        }
        // RFC 3501 Section 6.1.1 / RFC 9051 Section 6.1.1
        Command::Capability => {
            encode_simple(buf, tag, "CAPABILITY");
        }
        // RFC 3501 Section 6.1.2 / RFC 9051 Section 6.1.2
        Command::Noop => {
            encode_simple(buf, tag, "NOOP");
        }
        Command::Id(params) => {
            // RFC 2971 Section 2: ID requires ID capability.
            if !opts.has_capability(&Capability::Id) {
                return Err(EncodeError::MissingCapability {
                    cmd: "ID",
                    cap: "ID".into(),
                });
            }
            encode_id(buf, tag, params, utf8, literal_mode)?;
        }
        Command::GetMetadata {
            mailbox,
            entries,
            max_size,
            depth,
        } => {
            // RFC 5464 Section 1: METADATA or METADATA-SERVER capability required.
            if !opts.has_capability(&Capability::Metadata)
                && !opts.has_capability(&Capability::MetadataServer)
            {
                return Err(EncodeError::MissingCapability {
                    cmd: "GETMETADATA",
                    cap: "METADATA".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_getmetadata(
                buf,
                tag,
                &wire,
                entries,
                *max_size,
                depth.as_deref(),
                utf8,
                literal_mode,
            )?;
        }
        Command::SetMetadata { mailbox, entries } => {
            // RFC 5464 Section 1: METADATA or METADATA-SERVER capability required.
            if !opts.has_capability(&Capability::Metadata)
                && !opts.has_capability(&Capability::MetadataServer)
            {
                return Err(EncodeError::MissingCapability {
                    cmd: "SETMETADATA",
                    cap: "METADATA".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_setmetadata(buf, tag, &wire, entries, utf8, literal_mode)?;
        }
        Command::Thread {
            algorithm,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(
                buf,
                tag,
                "THREAD",
                algorithm,
                charset,
                criteria,
                false,
                literal_mode,
            )?;
        }
        Command::UidThread {
            algorithm,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(
                buf,
                tag,
                "UID THREAD",
                algorithm,
                charset,
                criteria,
                false,
                literal_mode,
            )?;
        }
        Command::Sort {
            sort_criteria,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(
                buf,
                tag,
                "SORT",
                sort_criteria,
                charset,
                criteria,
                true,
                literal_mode,
            )?;
        }
        Command::UidSort {
            sort_criteria,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(
                buf,
                tag,
                "UID SORT",
                sort_criteria,
                charset,
                criteria,
                true,
                literal_mode,
            )?;
        }
        Command::Compress => {
            // RFC 4978 Section 4: COMPRESS DEFLATE requires COMPRESS=DEFLATE.
            if !opts.has_capability(&Capability::CompressDeflate) {
                return Err(EncodeError::MissingCapability {
                    cmd: "COMPRESS",
                    cap: "COMPRESS=DEFLATE".into(),
                });
            }
            buf.extend_from_slice(tag.as_bytes());
            buf.extend_from_slice(b" COMPRESS DEFLATE\r\n");
        }

        // --- QUOTA (RFC 2087) ---
        Command::GetQuota { root } => {
            // RFC 2087 Section 4.2: GETQUOTA requires QUOTA capability.
            if !opts.has_capability(&Capability::Quota) {
                return Err(EncodeError::MissingCapability {
                    cmd: "GETQUOTA",
                    cap: "QUOTA".into(),
                });
            }
            encode_mailbox_cmd(buf, tag, "GETQUOTA", root, utf8, literal_mode);
        }
        Command::GetQuotaRoot { mailbox } => {
            // RFC 2087 Section 4.3: GETQUOTAROOT requires QUOTA capability.
            if !opts.has_capability(&Capability::Quota) {
                return Err(EncodeError::MissingCapability {
                    cmd: "GETQUOTAROOT",
                    cap: "QUOTA".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "GETQUOTAROOT", &wire, utf8, literal_mode);
        }
        Command::SetQuota { root, resources } => {
            // RFC 9208 Section 4.1.3: SETQUOTA requires QUOTASET capability.
            if !opts.has_capability(&Capability::QuotaSet) {
                return Err(EncodeError::MissingCapability {
                    cmd: "SETQUOTA",
                    cap: "QUOTASET".into(),
                });
            }
            encode_set_quota(buf, tag, root, resources, utf8, literal_mode)?;
        }

        // --- ACL (RFC 4314) ---
        Command::SetAcl {
            mailbox,
            identifier,
            rights,
        } => {
            // RFC 4314 Section 3.1: SETACL requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(EncodeError::MissingCapability {
                    cmd: "SETACL",
                    cap: "ACL".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_set_acl(buf, tag, &wire, identifier, rights, utf8, literal_mode);
        }
        Command::DeleteAcl {
            mailbox,
            identifier,
        } => {
            // RFC 4314 Section 3.2: DELETEACL requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(EncodeError::MissingCapability {
                    cmd: "DELETEACL",
                    cap: "ACL".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_two_quoted_args(buf, tag, "DELETEACL", &wire, identifier, utf8, literal_mode);
        }
        Command::GetAcl { mailbox } => {
            // RFC 4314 Section 3.3: GETACL requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(EncodeError::MissingCapability {
                    cmd: "GETACL",
                    cap: "ACL".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "GETACL", &wire, utf8, literal_mode);
        }
        Command::ListRights {
            mailbox,
            identifier,
        } => {
            // RFC 4314 Section 3.4: LISTRIGHTS requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(EncodeError::MissingCapability {
                    cmd: "LISTRIGHTS",
                    cap: "ACL".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_two_quoted_args(
                buf,
                tag,
                "LISTRIGHTS",
                &wire,
                identifier,
                utf8,
                literal_mode,
            );
        }
        Command::MyRights { mailbox } => {
            // RFC 4314 Section 3.5: MYRIGHTS requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(EncodeError::MissingCapability {
                    cmd: "MYRIGHTS",
                    cap: "ACL".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "MYRIGHTS", &wire, utf8, literal_mode);
        }

        // --- NOTIFY (RFC 5465) ---
        Command::NotifySet(params) => {
            // RFC 5465 Section 3: NOTIFY requires NOTIFY capability.
            if !opts.has_capability(&Capability::Notify) {
                return Err(EncodeError::MissingCapability {
                    cmd: "NOTIFY SET",
                    cap: "NOTIFY".into(),
                });
            }
            encode_notify_set(buf, tag, params, utf8, literal_mode)?;
        }
        Command::NotifyNone => {
            // RFC 5465 Section 3: NOTIFY requires NOTIFY capability.
            if !opts.has_capability(&Capability::Notify) {
                return Err(EncodeError::MissingCapability {
                    cmd: "NOTIFY NONE",
                    cap: "NOTIFY".into(),
                });
            }
            encode_simple(buf, tag, "NOTIFY NONE");
        }
    }
    Ok(())
}
