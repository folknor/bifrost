//! Internal command representations.
//!
//! These are constructed by `ImapConnection` methods and serialized by the encoder.
//! Not part of the public API.
//!
//! Client commands are defined in RFC 3501 Section 6 / RFC 9051 Section 6.

use std::fmt;
use std::ops::Deref;

use super::flag::Flag;
use super::mailbox::MailboxAttribute;
use super::validated::{MailboxName, SequenceSet};
use zeroize::Zeroizing;

/// Internal zeroizing string for credentials and SASL payloads.
#[derive(Clone, Default, PartialEq, Eq)]
pub(crate) struct SecretString(Zeroizing<String>);

impl SecretString {
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl From<String> for SecretString {
    fn from(value: String) -> Self {
        Self(Zeroizing::new(value))
    }
}

impl From<&str> for SecretString {
    fn from(value: &str) -> Self {
        Self(Zeroizing::new(value.to_owned()))
    }
}

impl Deref for SecretString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for SecretString {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// An IMAP command to be encoded and sent to the server.
#[derive(Debug, Clone)]
pub(crate) enum Command {
    // --- Authentication (RFC 3501 Section 6.2 / RFC 9051 Section 6.2) ---
    /// LOGIN command (RFC 3501 Section 6.2.3).
    Login { user: String, pass: SecretString },
    /// AUTHENTICATE command (RFC 3501 Section 6.2.2, RFC 4959 for SASL-IR).
    Authenticate {
        mechanism: String,
        initial_response: Option<SecretString>,
    },
    /// STARTTLS command (RFC 3501 Section 6.2.1).
    StartTls,
    /// LOGOUT command (RFC 3501 Section 6.1.3).
    Logout,
    /// UNAUTHENTICATE command (RFC 8437 Section 2).
    Unauthenticate,
    /// ENABLE command (RFC 5161 Section 3).
    ///
    /// Requests the server to enable one or more IMAP extensions.
    /// Each element is a capability atom (e.g., `"CONDSTORE"`, `"QRESYNC"`).
    /// The server responds with an untagged `ENABLED` listing the
    /// extensions it actually activated.
    Enable { capabilities: Vec<String> },

    // --- Mailbox management (RFC 3501 Section 6.3 / RFC 9051 Section 6.3) ---
    /// LIST command (RFC 3501 Section 6.3.8, RFC 5258 for LIST-EXTENDED).
    List { reference: String, pattern: String },
    /// LIST with selection and/or return options (RFC 5258 Section 3 / RFC 9051 Section 6.3.9).
    ///
    /// RFC 5258 Section 3 extends LIST to allow:
    /// `LIST [select-opts] reference mailbox-pattern / pattern-list [return-opts]`
    /// where selection options appear before the reference name, multiple
    /// mailbox patterns are wrapped in `(<pattern1> <pattern2> ...)`, and
    /// return options are encoded as `RETURN (<opt1> <opt2> ...)`.
    ///
    /// `selection_options` and `return_options` are raw option tokens or
    /// option phrases (for example `STATUS (MESSAGES UNSEEN)`), already
    /// validated by the connection layer.
    ListExtended {
        selection_options: Vec<String>,
        reference: String,
        patterns: Vec<String>,
        return_options: Vec<String>,
    },
    /// LIST with STATUS return option (RFC 5819 Section 2).
    ///
    /// Requests both LIST and STATUS data in a single command.
    /// The server interleaves LIST and STATUS untagged responses.
    ListStatus {
        reference: String,
        pattern: String,
        status_items: String,
    },
    /// SELECT command (RFC 3501 Section 6.3.1, RFC 7162 Section 3.1.1/3.2.5.2).
    Select {
        mailbox: MailboxName,
        /// Optional CONDSTORE parameter (RFC 7162 Section 3.1.1).
        condstore: bool,
        /// Optional QRESYNC parameters (RFC 7162 Section 3.2.5.2).
        qresync: Option<super::QresyncParams>,
    },
    /// EXAMINE command (RFC 3501 Section 6.3.2, RFC 7162 Section 3.1.1/3.2.5.2).
    Examine {
        mailbox: MailboxName,
        /// Optional CONDSTORE parameter (RFC 7162 Section 3.1.1).
        condstore: bool,
        /// Optional QRESYNC parameters (RFC 7162 Section 3.2.5.2).
        qresync: Option<super::QresyncParams>,
    },
    /// CREATE command (RFC 3501 Section 6.3.3).
    Create { mailbox: MailboxName },
    /// CREATE with USE special-use attributes (RFC 6154 Section 3).
    ///
    /// RFC 6154 Section 3 / Section 6 ABNF:
    /// `create-param =/ "USE" SP "(" [use-attr *(SP use-attr)] ")"`
    /// where `use-attr = "\All" / "\Archive" / "\Drafts" / "\Flagged" /
    ///                    "\Junk" / "\Sent" / "\Trash" / use-attr-ext`
    ///
    /// Clients MUST NOT use the USE parameter unless the server advertises
    /// the CREATE-SPECIAL-USE capability.
    CreateSpecialUse {
        mailbox: MailboxName,
        special_use: Vec<MailboxAttribute>,
    },
    /// DELETE command (RFC 3501 Section 6.3.4).
    Delete { mailbox: MailboxName },
    /// RENAME command (RFC 3501 Section 6.3.5).
    Rename {
        mailbox: MailboxName,
        new_name: MailboxName,
    },
    /// SUBSCRIBE command (RFC 3501 Section 6.3.6).
    Subscribe { mailbox: MailboxName },
    /// UNSUBSCRIBE command (RFC 3501 Section 6.3.7).
    Unsubscribe { mailbox: MailboxName },
    /// LSUB command (RFC 3501 Section 6.3.9).
    Lsub { reference: String, pattern: String },
    /// CLOSE command (RFC 3501 Section 6.4.2).
    Close,
    /// UNSELECT command (RFC 3691 Section 3).
    ///
    /// Closes the currently selected mailbox without expunging deleted
    /// messages  -  unlike CLOSE which implicitly expunges.
    Unselect,
    /// STATUS command (RFC 3501 Section 6.3.10).
    Status { mailbox: MailboxName, items: String },
    /// NAMESPACE command (RFC 2342).
    Namespace,
    /// CHECK command (RFC 3501 Section 6.4.1).
    Check,

    // --- Message operations  -  UID variants (RFC 3501 Section 6.4) ---
    /// SEARCH command (RFC 3501 Section 6.4.4).
    Search { criteria: String },
    /// SEARCH RETURN (opts) command (RFC 4731 Section 3.2).
    ///
    /// RFC 4731 Section 3.2 / Section 4 ABNF:
    /// `search-return-opt = "MIN" / "MAX" / "ALL" / "COUNT"`
    /// `"SEARCH" [SP "RETURN" SP "(" [search-return-opt *(SP search-return-opt)] ")"]
    ///  SP search-criteria`
    SearchReturn {
        criteria: String,
        return_opts: Vec<String>,
    },
    /// SEARCH RETURN (SAVE) command (RFC 5182 Section 2).
    ///
    /// Saves the search results server-side as `$` for use in subsequent commands.
    SearchSave { criteria: String },
    /// FETCH command (RFC 3501 Section 6.4.5, RFC 7162 Section 3.1.4.1 for CHANGEDSINCE).
    Fetch {
        sequence_set: SequenceSet,
        items: String,
        /// Optional CHANGEDSINCE modifier (RFC 7162 Section 3.1.4.1).
        changed_since: Option<u64>,
    },
    /// STORE command (RFC 3501 Section 6.4.6, RFC 7162 for CONDSTORE).
    Store {
        sequence_set: SequenceSet,
        operation: super::StoreOperation,
        flags: Vec<Flag>,
        unchanged_since: Option<u64>,
    },
    /// COPY command (RFC 3501 Section 6.4.7).
    Copy {
        sequence_set: SequenceSet,
        mailbox: MailboxName,
    },
    /// MOVE command (RFC 6851 Section 3).
    Move {
        sequence_set: SequenceSet,
        mailbox: MailboxName,
    },
    /// UID SEARCH command (RFC 3501 Section 6.4.4).
    UidSearch { criteria: String },
    /// UID SEARCH RETURN (opts) command (RFC 4731 Section 3.2).
    ///
    /// RFC 4731 Section 3.2 / Section 4 ABNF:
    /// `search-return-opt = "MIN" / "MAX" / "ALL" / "COUNT"`
    /// `"UID" SP "SEARCH" [SP "RETURN" SP "(" [search-return-opt *(SP search-return-opt)] ")"]
    ///  SP search-criteria`
    UidSearchReturn {
        criteria: String,
        return_opts: Vec<String>,
    },
    /// UID SEARCH RETURN (SAVE) command (RFC 5182 Section 2).
    ///
    /// Saves the search results server-side as `$` for use in subsequent commands.
    UidSearchSave { criteria: String },
    /// UID FETCH command (RFC 3501 Section 6.4.5, RFC 7162 Section 3.1.4.1 for CHANGEDSINCE,
    /// RFC 7162 Section 3.2.6 for VANISHED).
    UidFetch {
        sequence_set: SequenceSet,
        items: String,
        /// Optional CHANGEDSINCE modifier (RFC 7162 Section 3.1.4.1).
        changed_since: Option<u64>,
        /// VANISHED modifier for QRESYNC (RFC 7162 Section 3.2.6).
        ///
        /// When `true`, the server returns `VANISHED` responses instead of
        /// `EXPUNGE` for expunged messages. Requires `CHANGEDSINCE` to also
        /// be set, and QRESYNC must have been `ENABLE`d first.
        vanished: bool,
    },
    /// UID STORE command (RFC 3501 Section 6.4.6, RFC 7162 for CONDSTORE).
    UidStore {
        sequence_set: SequenceSet,
        operation: super::StoreOperation,
        flags: Vec<Flag>,
        unchanged_since: Option<u64>,
    },
    /// UID MOVE command (RFC 6851).
    UidMove {
        sequence_set: SequenceSet,
        mailbox: MailboxName,
    },
    /// UID COPY command (RFC 3501 Section 6.4.7).
    UidCopy {
        sequence_set: SequenceSet,
        mailbox: MailboxName,
    },
    /// UID EXPUNGE command (RFC 4315 UIDPLUS).
    UidExpunge { sequence_set: SequenceSet },
    /// EXPUNGE command (RFC 3501 Section 6.4.3).
    Expunge,

    // --- IDLE (RFC 2177) ---
    /// IDLE command (RFC 2177).
    Idle,

    // --- Misc ---
    /// CAPABILITY command (RFC 3501 Section 6.1.1).
    Capability,
    /// NOOP command (RFC 3501 Section 6.1.2).
    Noop,
    /// ID command (RFC 2971 Section 3.1).
    ///
    /// Values are `nstring` per RFC 2971 Section 3.1: `id_params_list ::= "(" #(string SPACE nstring) ")"`.
    /// A `None` value encodes as `NIL` on the wire.
    Id(Vec<(String, Option<String>)>),

    // --- METADATA (RFC 5464) ---
    /// GETMETADATA command (RFC 5464 Section 4.2).
    GetMetadata {
        mailbox: MailboxName,
        entries: Vec<String>,
        /// Maximum size of returned values in bytes (RFC 5464 Section 4.2.2).
        max_size: Option<u64>,
        /// Depth of entry hierarchy: `"0"`, `"1"`, or `"infinity"` (RFC 5464 Section 4.2.2).
        depth: Option<String>,
    },
    /// SETMETADATA command (RFC 5464 Section 4.3).
    ///
    /// Each entry is a `(name, value)` pair. `None` value deletes the entry.
    /// RFC 5464 Section 5: `value = nstring / literal8`  -  values are raw bytes
    /// to support binary data via the `literal8` syntax.
    SetMetadata {
        mailbox: MailboxName,
        entries: Vec<(String, Option<Vec<u8>>)>,
    },

    // --- THREAD (RFC 5256) ---
    /// THREAD command (RFC 5256 Section 3).
    Thread {
        algorithm: String,
        charset: String,
        criteria: String,
    },
    /// UID THREAD command (RFC 5256 Section 3).
    UidThread {
        algorithm: String,
        charset: String,
        criteria: String,
    },

    // --- SORT (RFC 5256) ---
    /// SORT command (RFC 5256 Section 2).
    Sort {
        /// Sort criteria: one or more sort keys per RFC 5256 Section 2
        /// `sort-criteria = "(" sort-key *(SP sort-key) ")"`.
        sort_criteria: String,
        charset: String,
        criteria: String,
    },
    /// UID SORT command (RFC 5256 Section 2).
    UidSort {
        /// Sort criteria: one or more sort keys per RFC 5256 Section 2
        /// `sort-criteria = "(" sort-key *(SP sort-key) ")"`.
        sort_criteria: String,
        charset: String,
        criteria: String,
    },

    // --- NOTIFY (RFC 5465) ---
    /// NOTIFY SET command (RFC 5465 Section 3).
    ///
    /// Registers interest in events for specified mailbox filters.
    /// Replaces any previous NOTIFY configuration.
    NotifySet(super::notify::NotifySetParams),
    /// NOTIFY NONE command (RFC 5465 Section 3).
    ///
    /// Cancels all event subscriptions, reverting to baseline IMAP behavior.
    NotifyNone,

    // --- COMPRESS (RFC 4978) ---
    /// COMPRESS command (RFC 4978 Section 4).
    Compress,

    // --- QUOTA (RFC 2087) ---
    /// GETQUOTA command (RFC 2087 Section 4.2).
    GetQuota { root: String },
    /// GETQUOTAROOT command (RFC 2087 Section 4.3).
    GetQuotaRoot { mailbox: MailboxName },
    /// SETQUOTA command (RFC 2087 Section 4.1).
    ///
    /// Sets resource limits on a quota root. Each element of `resources` is
    /// a `(resource_name, limit)` pair  -  e.g. `("STORAGE", 51200)`.
    SetQuota {
        root: String,
        resources: Vec<(String, u64)>,
    },

    // --- ACL (RFC 4314) ---
    /// SETACL command (RFC 4314 Section 3.1).
    SetAcl {
        mailbox: MailboxName,
        identifier: String,
        rights: String,
    },
    /// DELETEACL command (RFC 4314 Section 3.2).
    DeleteAcl {
        mailbox: MailboxName,
        identifier: String,
    },
    /// GETACL command (RFC 4314 Section 3.3).
    GetAcl { mailbox: MailboxName },
    /// LISTRIGHTS command (RFC 4314 Section 3.4).
    ListRights {
        mailbox: MailboxName,
        identifier: String,
    },
    /// MYRIGHTS command (RFC 4314 Section 3.5).
    MyRights { mailbox: MailboxName },
}

/// Classification-level command discriminant.
///
/// Used by [`crate::codec::classification::classify`] to route untagged
/// responses. UID variants map to their non-UID counterpart because they
/// produce identical solicited response sets. Similarly, `CreateSpecialUse`
/// maps to `Create`, `ListExtended` maps to `List`, and `Done` maps to
/// `Idle`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CommandKind {
    // --- Any state (RFC 3501 Section6.1) ---
    /// CAPABILITY (RFC 3501 Section6.1.1).
    Capability,
    /// NOOP (RFC 3501 Section6.1.2).
    Noop,
    /// LOGOUT (RFC 3501 Section6.1.3).
    Logout,

    // --- Not authenticated (RFC 3501 Section6.2) ---
    /// LOGIN (RFC 3501 Section6.2.3).
    Login,
    /// AUTHENTICATE (RFC 3501 Section6.2.2).
    Authenticate,
    /// STARTTLS (RFC 3501 Section6.2.1).
    StartTls,

    // --- Authenticated (RFC 3501 Section6.3) ---
    /// ENABLE (RFC 5161 Section3).
    Enable,
    /// SELECT (RFC 3501 Section6.3.1).
    Select,
    /// EXAMINE (RFC 3501 Section6.3.2).
    Examine,
    /// CREATE (RFC 3501 Section6.3.3), including CREATE-SPECIAL-USE (RFC 6154 Section3).
    Create,
    /// DELETE (RFC 3501 Section6.3.4).
    Delete,
    /// RENAME (RFC 3501 Section6.3.5).
    Rename,
    /// SUBSCRIBE (RFC 3501 Section6.3.6).
    Subscribe,
    /// UNSUBSCRIBE (RFC 3501 Section6.3.7).
    Unsubscribe,
    /// LIST (RFC 3501 Section6.3.8) and LIST-EXTENDED (RFC 5258 Section3).
    List,
    /// LIST with STATUS return option (RFC 5819 Section2).
    ListStatus,
    /// LSUB (RFC 3501 Section6.3.9).
    Lsub,
    /// STATUS (RFC 3501 Section6.3.10).
    Status,
    /// NAMESPACE (RFC 2342).
    Namespace,
    /// APPEND / MULTIAPPEND (RFC 3501 Section6.3.11 / RFC 3502).
    Append,

    // --- Selected (RFC 3501 Section6.4) ---
    /// CHECK (RFC 3501 Section6.4.1).
    Check,
    /// CLOSE (RFC 3501 Section6.4.2).
    Close,
    /// UNSELECT (RFC 3691 Section3).
    Unselect,
    /// EXPUNGE / UID EXPUNGE (RFC 3501 Section6.4.3 / RFC 4315).
    Expunge,
    /// SEARCH / UID SEARCH (RFC 3501 Section6.4.4).
    Search,
    /// SEARCH RETURN / UID SEARCH RETURN (RFC 4731 Section3.2).
    SearchReturn,
    /// SEARCH RETURN (SAVE) / UID SEARCH RETURN (SAVE) (RFC 5182 Section2).
    SearchSave,
    /// FETCH / UID FETCH (RFC 3501 Section6.4.5).
    Fetch,
    /// STORE / UID STORE (RFC 3501 Section6.4.6).
    Store,
    /// COPY / UID COPY (RFC 3501 Section6.4.7).
    Copy,
    /// MOVE / UID MOVE (RFC 6851 Section3).
    Move,

    // --- IDLE (RFC 2177) ---
    /// IDLE (RFC 2177). `Done` also maps here.
    Idle,

    // --- Extensions ---
    /// ID (RFC 2971).
    Id,
    /// GETMETADATA (RFC 5464 Section4.2).
    GetMetadata,
    /// SETMETADATA (RFC 5464 Section4.3).
    SetMetadata,
    /// THREAD / UID THREAD (RFC 5256 Section3).
    Thread,
    /// SORT / UID SORT (RFC 5256 Section2).
    Sort,
    /// NOTIFY SET (RFC 5465 Section3).
    NotifySet,
    /// NOTIFY NONE (RFC 5465 Section3).
    NotifyNone,
    /// COMPRESS (RFC 4978 Section4).
    Compress,
    /// UNAUTHENTICATE (RFC 8437 Section2).
    Unauthenticate,
    /// GETQUOTA (RFC 2087 Section4.2).
    GetQuota,
    /// GETQUOTAROOT (RFC 2087 Section4.3).
    GetQuotaRoot,
    /// SETQUOTA (RFC 2087 Section4.1).
    SetQuota,
    /// SETACL (RFC 4314 Section3.1).
    SetAcl,
    /// DELETEACL (RFC 4314 Section3.2).
    DeleteAcl,
    /// GETACL (RFC 4314 Section3.3).
    GetAcl,
    /// LISTRIGHTS (RFC 4314 Section3.4).
    ListRights,
    /// MYRIGHTS (RFC 4314 Section3.5).
    MyRights,
}

impl Command {
    /// Return the mailbox target of this command, if applicable.
    ///
    /// Used by the dispatcher to populate `ClassificationContext::command_target`
    /// and `ConsumerContext::command_target` for response correlation.
    /// Commands like SELECT, STATUS, GETMETADATA, etc. have an explicit
    /// mailbox argument that the classification layer needs to match
    /// solicited STATUS responses to the correct command (RFC 3501 Section6.3.10).
    pub(crate) fn mailbox_target(&self) -> Option<&MailboxName> {
        match self {
            Self::Select { mailbox, .. }
            | Self::Examine { mailbox, .. }
            | Self::Create { mailbox, .. }
            | Self::CreateSpecialUse { mailbox, .. }
            | Self::Delete { mailbox }
            | Self::Rename { mailbox, .. }
            | Self::Subscribe { mailbox }
            | Self::Unsubscribe { mailbox }
            | Self::Status { mailbox, .. }
            | Self::GetMetadata { mailbox, .. }
            | Self::SetMetadata { mailbox, .. }
            | Self::Copy { mailbox, .. }
            | Self::Move { mailbox, .. }
            | Self::UidCopy { mailbox, .. }
            | Self::UidMove { mailbox, .. }
            | Self::GetAcl { mailbox }
            | Self::SetAcl { mailbox, .. }
            | Self::DeleteAcl { mailbox, .. }
            | Self::ListRights { mailbox, .. }
            | Self::MyRights { mailbox }
            | Self::GetQuotaRoot { mailbox } => Some(mailbox),
            _ => None,
        }
    }

    /// Return the classification-level discriminant for this command.
    ///
    /// UID variants map to their non-UID counterpart because they produce
    /// identical solicited response sets.
    pub(crate) fn kind(&self) -> CommandKind {
        match self {
            // Any state (RFC 3501 Section6.1)
            Self::Capability => CommandKind::Capability,
            Self::Noop => CommandKind::Noop,
            Self::Logout => CommandKind::Logout,
            Self::Enable { .. } => CommandKind::Enable,

            // Not authenticated (RFC 3501 Section6.2)
            Self::Login { .. } => CommandKind::Login,
            Self::Authenticate { .. } => CommandKind::Authenticate,
            Self::StartTls => CommandKind::StartTls,

            // Authenticated (RFC 3501 Section6.3)
            Self::Select { .. } => CommandKind::Select,
            Self::Examine { .. } => CommandKind::Examine,
            Self::Create { .. } | Self::CreateSpecialUse { .. } => CommandKind::Create,
            Self::Delete { .. } => CommandKind::Delete,
            Self::Rename { .. } => CommandKind::Rename,
            Self::Subscribe { .. } => CommandKind::Subscribe,
            Self::Unsubscribe { .. } => CommandKind::Unsubscribe,
            Self::List { .. } | Self::ListExtended { .. } => CommandKind::List,
            Self::ListStatus { .. } => CommandKind::ListStatus,
            Self::Lsub { .. } => CommandKind::Lsub,
            Self::Status { .. } => CommandKind::Status,
            Self::Namespace => CommandKind::Namespace,

            // Selected (RFC 3501 Section6.4)
            Self::Check => CommandKind::Check,
            Self::Close => CommandKind::Close,
            Self::Unselect => CommandKind::Unselect,
            Self::Expunge | Self::UidExpunge { .. } => CommandKind::Expunge,
            Self::Search { .. } | Self::UidSearch { .. } => CommandKind::Search,
            Self::SearchReturn { .. } | Self::UidSearchReturn { .. } => CommandKind::SearchReturn,
            Self::SearchSave { .. } | Self::UidSearchSave { .. } => CommandKind::SearchSave,
            Self::Fetch { .. } | Self::UidFetch { .. } => CommandKind::Fetch,
            Self::Store { .. } | Self::UidStore { .. } => CommandKind::Store,
            Self::Copy { .. } | Self::UidCopy { .. } => CommandKind::Copy,
            Self::Move { .. } | Self::UidMove { .. } => CommandKind::Move,

            // IDLE (RFC 2177)
            Self::Idle => CommandKind::Idle,

            // Extensions
            Self::Id(_) => CommandKind::Id,
            Self::GetMetadata { .. } => CommandKind::GetMetadata,
            Self::SetMetadata { .. } => CommandKind::SetMetadata,
            Self::Thread { .. } | Self::UidThread { .. } => CommandKind::Thread,
            Self::Sort { .. } | Self::UidSort { .. } => CommandKind::Sort,
            Self::NotifySet(_) => CommandKind::NotifySet,
            Self::NotifyNone => CommandKind::NotifyNone,
            Self::Compress => CommandKind::Compress,
            Self::Unauthenticate => CommandKind::Unauthenticate,
            Self::GetQuota { .. } => CommandKind::GetQuota,
            Self::GetQuotaRoot { .. } => CommandKind::GetQuotaRoot,
            Self::SetQuota { .. } => CommandKind::SetQuota,
            Self::SetAcl { .. } => CommandKind::SetAcl,
            Self::DeleteAcl { .. } => CommandKind::DeleteAcl,
            Self::GetAcl { .. } => CommandKind::GetAcl,
            Self::ListRights { .. } => CommandKind::ListRights,
            Self::MyRights { .. } => CommandKind::MyRights,
        }
    }
}

#[cfg(test)]
#[path = "command_tests.rs"]
mod tests;
