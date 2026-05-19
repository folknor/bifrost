//! IMAP server response types (RFC 3501 Section 7 / RFC 9051 Section 7).
//!
//! Models the full grammar of IMAP responses: greeting, tagged, untagged, and continuation.
//! Extended response codes per RFC 5530.

use super::validated::MailboxName;
use super::{FetchResponse, Flag, MailboxInfo, StatusItem};

/// A complete response line from the IMAP server
/// (RFC 3501 Section 2.2.2 / RFC 9051 Section 2.2.2).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Response {
    /// Initial server greeting on connection (RFC 3501 Section 7.1).
    Greeting(GreetingResponse),
    /// Tagged response to a client command (RFC 3501 Section 2.2.2).
    Tagged(TaggedResponse),
    /// Untagged (unsolicited or data) response (RFC 3501 Section 2.2.2).
    Untagged(Box<UntaggedResponse>),
    /// Continuation request (`+ ...`) (RFC 3501 Section 7.5).
    Continuation(ContinuationRequest),
}

/// Initial greeting sent by the server upon connection
/// (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct GreetingResponse {
    /// Greeting status (`OK`, `PREAUTH`, or `BYE`) (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
    pub status: GreetingStatus,
    /// Optional response code in square brackets (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
    pub code: Option<ResponseCode>,
    /// Human-readable text following the status (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
    pub text: String,
}

/// Status of the server greeting (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum GreetingStatus {
    /// `* OK`  -  server ready, client should authenticate.
    #[default]
    Ok,
    /// `* PREAUTH`  -  already authenticated (e.g. via TLS client cert).
    PreAuth,
    /// `* BYE`  -  server refusing connections.
    Bye,
}

/// Tagged response (response to a specific client command)
/// (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TaggedResponse {
    /// Command tag that this response corresponds to (RFC 3501 Section 2.2.1 / RFC 9051 Section 2.2.1).
    pub tag: String,
    /// Completion status (`OK`, `NO`, or `BAD`) (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
    pub status: StatusKind,
    /// Optional response code in square brackets (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
    pub code: Option<ResponseCode>,
    /// Human-readable text following the status (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
    pub text: String,
}

impl TaggedResponse {
    /// Check that the response indicates `OK` status. On success returns the
    /// response itself so callers can still access fields like `code`. On
    /// failure returns an appropriate [`Error`] for `NO` / `BAD`.
    ///
    /// RFC 3501 Section 7.1 / RFC 9051 Section 7.1: `OK` indicates success,
    /// `NO` an operational error, `BAD` a protocol-level error.
    pub(crate) fn require_ok(self) -> Result<Self, crate::error::Error> {
        match self.status {
            StatusKind::Ok => Ok(self),
            StatusKind::No => Err(crate::error::Error::no_with_code(self.text, self.code)),
            StatusKind::Bad => Err(crate::error::Error::bad_with_code(self.text, self.code)),
        }
    }
}

/// Status of a tagged response (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum StatusKind {
    #[default]
    Ok,
    No,
    Bad,
}

/// Status of an untagged status response (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UntaggedStatus {
    #[default]
    Ok,
    No,
    Bad,
    Bye,
}

/// Untagged server response (RFC 3501 Section 7 / RFC 9051 Section 7).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UntaggedResponse {
    /// `* OK/NO/BAD/BYE [code] text` (RFC 3501 Section 7.1).
    Status {
        status: UntaggedStatus,
        code: Option<ResponseCode>,
        text: String,
    },
    /// `* <n> EXISTS` (RFC 3501 Section 7.3.1).
    Exists(u32),
    /// `* <n> RECENT` (RFC 3501 Section 7.3.2).
    Recent(u32),
    /// `* <n> EXPUNGE` (RFC 3501 Section 7.4.1).
    Expunge(u32),
    /// `* <n> FETCH (...)` (RFC 3501 Section 7.4.2).
    Fetch(Box<FetchResponse>),
    /// `* LIST (\attrs) "/" "name"` (RFC 3501 Section 7.2.2).
    List(MailboxInfo),
    /// `* LSUB (\attrs) "/" "name"` (RFC 3501 Section 7.2.3).
    Lsub(MailboxInfo),
    /// `* FLAGS (...)` (RFC 3501 Section 7.2.6).
    Flags(Vec<Flag>),
    /// `* SEARCH 1 2 3 ... [(MODSEQ n)]` (RFC 3501 Section 7.2.5, RFC 7162 Section 3.1.5).
    ///
    /// The optional `mod_seq` is present when the client searched with a MODSEQ
    /// criterion and the result is non-empty (RFC 7162 Section 3.1.5).
    Search {
        /// Matching message sequence numbers or UIDs.
        uids: Vec<u32>,
        /// Highest mod-sequence of matching messages (RFC 7162 Section 3.1.5).
        mod_seq: Option<u64>,
    },
    /// `* ESEARCH (TAG "tag") [UID] result-data` (RFC 4731 Section 3.1).
    ///
    /// RFC 4731 Section 3.1 ABNF:
    /// `search-return-data = "MIN" SP nz-number / "MAX" SP nz-number /
    ///                        "ALL" SP sequence-set / "COUNT" SP number`
    Esearch(EsearchResponse),
    /// `* STATUS "mailbox" (...)` (RFC 3501 Section 7.2.4).
    MailboxStatus {
        mailbox: MailboxName,
        items: Vec<StatusItem>,
    },
    /// `* CAPABILITY ...` (RFC 3501 Section 7.2.1).
    Capability(Vec<Capability>),
    /// `* ENABLED ...` (RFC 5161 Section 3.2).
    Enabled(Vec<String>),
    /// `* VANISHED (EARLIER) 1:5` (RFC 7162 QRESYNC).
    Vanished { earlier: bool, uids: Vec<UidRange> },
    /// `* ID (...)` (RFC 2971 Section 3.2).
    Id(Vec<(String, Option<String>)>),
    /// `* NAMESPACE personal other shared` (RFC 2342).
    Namespace {
        personal: Vec<NamespaceDescriptor>,
        other: Vec<NamespaceDescriptor>,
        shared: Vec<NamespaceDescriptor>,
    },

    // --- QUOTA (RFC 2087) ---
    /// `* QUOTA <root> (STORAGE <usage> <limit>)` (RFC 2087 Section 5.1).
    Quota {
        root: String,
        resources: Vec<QuotaResource>,
    },
    /// `* QUOTAROOT <mailbox> <root1> <root2> ...` (RFC 2087 Section 5.2).
    QuotaRoot {
        mailbox: MailboxName,
        roots: Vec<String>,
    },

    // --- ACL (RFC 4314) ---
    /// `* ACL <mailbox> <id1> <rights1> ...` (RFC 4314 Section 3.6).
    Acl {
        mailbox: MailboxName,
        entries: Vec<AclEntry>,
    },
    /// `* MYRIGHTS <mailbox> <rights>` (RFC 4314 Section 3.8).
    MyRights {
        mailbox: MailboxName,
        rights: String,
    },
    /// `* LISTRIGHTS <mailbox> <id> <required> <optional1> ...` (RFC 4314 Section 3.7).
    ListRights {
        mailbox: MailboxName,
        identifier: String,
        required: String,
        optional: Vec<String>,
    },
    /// `* METADATA "mailbox" (entry1 value1 ...)` (RFC 5464 Section 4.4).
    Metadata {
        mailbox: MailboxName,
        entries: Vec<MetadataEntry>,
    },
    /// `* THREAD (...)` (RFC 5256 Section 4).
    Thread(Vec<ThreadNode>),
    /// SORT response  -  sorted message numbers and optional MODSEQ
    /// (RFC 5256 Section 4, RFC 7162 Section 3.1.6).
    ///
    /// RFC 7162 Section 3.1.6: when a MODSEQ search criterion is used and the
    /// SORT result is non-empty, the server appends `(MODSEQ <n>)`.
    Sort {
        /// Sorted message numbers or UIDs (RFC 5256 Section 4).
        nums: Vec<u32>,
        /// Highest mod-sequence value of matching messages (RFC 7162 Section 3.1.6).
        mod_seq: Option<u64>,
    },
    /// Unknown or unrecognized untagged response (RFC 9051 Section 2.2.2).
    ///
    /// Servers may send extension responses that this client does not yet
    /// implement. Per RFC 9051, clients MUST tolerate such responses.
    Unknown(String),
}

/// Continuation request from the server (RFC 3501 Section 7.5 / RFC 9051 Section 7.5).
///
/// RFC 3501 Section 7.5: `continue-req = "+" SP (resp-text / base64) CRLF`
/// where `resp-text = ["[" resp-text-code "]" SP] text`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ContinuationRequest {
    /// Optional response code in square brackets (RFC 3501 Section 7.5 / RFC 9051 Section 7.5).
    ///
    /// Present when the server sends a continuation like `+ [ALERT] text\r\n`.
    /// Base64 SASL challenges never start with `[`, so this is `None` for those.
    pub code: Option<ResponseCode>,
    /// Text or base64 challenge from the server
    /// (RFC 3501 Section 7.5 / RFC 9051 Section 7.5.1).
    pub data: String,
}

/// Response code in square brackets (e.g. `[UIDVALIDITY 12345]`)
/// (RFC 3501 Section 7.1 / RFC 9051 Section 7.1).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ResponseCode {
    /// `[ALERT]`  -  must be presented to the user (RFC 3501 Section 7.1).
    Alert,
    /// `[BADCHARSET (charsets)]`  -  search charset not supported (RFC 3501 Section 7.1).
    BadCharset(Vec<String>),
    /// `[CAPABILITY ...]`  -  capability list (RFC 3501 Section 7.1).
    Capability(Vec<Capability>),
    /// `[PARSE]`  -  message headers could not be parsed (RFC 3501 Section 7.1).
    Parse,
    /// `[PERMANENTFLAGS (flags)]`  -  flags the client can change permanently (RFC 3501 Section 7.1).
    PermanentFlags(Vec<Flag>),
    /// `[READ-ONLY]`  -  mailbox is read-only (RFC 3501 Section 7.1).
    ReadOnly,
    /// `[READ-WRITE]`  -  mailbox is read-write (RFC 3501 Section 7.1).
    ReadWrite,
    /// `[TRYCREATE]`  -  attempt to CREATE the target mailbox (RFC 3501 Section 7.1).
    TryCreate,
    /// `[UIDNEXT n]`  -  predicted next UID (RFC 3501 Section 7.1).
    UidNext(u32),
    /// `[UIDVALIDITY n]`  -  UID validity value (RFC 3501 Section 7.1).
    UidValidity(u32),
    /// `[UNSEEN n]`  -  first unseen message sequence number (RFC 3501 Section 7.1).
    Unseen(u32),
    /// `[APPENDUID uidvalidity uid-set]` (RFC 4315 UIDPLUS Section 3).
    ///
    /// For a single APPEND, `uids` contains one range. For MULTIAPPEND
    /// (RFC 3502), `uids` may contain multiple ranges.
    AppendUid {
        uid_validity: u32,
        uids: Vec<UidRange>,
    },
    /// `[COPYUID uidvalidity source-uids dest-uids]` (RFC 4315 UIDPLUS).
    CopyUid {
        uid_validity: u32,
        source_uids: Vec<UidRange>,
        dest_uids: Vec<UidRange>,
    },
    /// `[HIGHESTMODSEQ n]` (RFC 7162 CONDSTORE).
    HighestModSeq(u64),
    /// `[MODIFIED sequence-set]` (RFC 7162 Section 3.1.3 / Section 7).
    ///
    /// For `STORE`, the set contains message sequence numbers. For `UID STORE`,
    /// it contains UIDs.
    Modified(Vec<UidRange>),
    /// `[NOMODSEQ]`  -  mailbox does not support mod-sequences (RFC 7162 Section 3.1.2).
    NoModSeq,
    /// `[CLOSED]`  -  previously selected mailbox is now closed (RFC 7162 QRESYNC).
    Closed,
    /// `[MAILBOXID (objectid)]`  -  unique mailbox identifier (RFC 8474 Section 5.1).
    MailboxId(String),

    // --- RFC 5530 extended response codes ---
    /// `[UNAVAILABLE]`  -  server temporarily unavailable (RFC 5530 Section 3).
    Unavailable,
    /// `[AUTHENTICATIONFAILED]`  -  authentication credentials invalid (RFC 5530 Section 3).
    AuthenticationFailed,
    /// `[AUTHORIZATIONFAILED]`  -  authorization identity not permitted (RFC 5530 Section 3).
    AuthorizationFailed,
    /// `[EXPIRED]`  -  credentials have expired (RFC 5530 Section 3).
    Expired,
    /// `[PRIVACYREQUIRED]`  -  operation requires encryption (RFC 5530 Section 3).
    PrivacyRequired,
    /// `[CONTACTADMIN]`  -  contact server administrator (RFC 5530 Section 3).
    ContactAdmin,
    /// `[NOPERM]`  -  no permission to perform the operation (RFC 5530 Section 3).
    NoPerm,
    /// `[INUSE]`  -  resource is in use by another session (RFC 5530 Section 3).
    InUse,
    /// `[EXPUNGEISSUED]`  -  expunge occurred during operation (RFC 5530 Section 3).
    ExpungeIssued,
    /// `[CORRUPTION]`  -  server detected data corruption (RFC 5530 Section 3).
    Corruption,
    /// `[SERVERBUG]`  -  server encountered an internal bug (RFC 5530 Section 3).
    ServerBug,
    /// `[CLIENTBUG]`  -  client sent malformed or nonsensical data (RFC 5530 Section 3).
    ClientBug,
    /// `[CANNOT]`  -  operation is not supported on this mailbox/server (RFC 5530 Section 3).
    Cannot,
    /// `[LIMIT]`  -  operation exceeds a server-imposed limit (RFC 5530 Section 3).
    Limit,
    /// `[OVERQUOTA]`  -  user has exceeded their storage quota (RFC 5530 Section 3).
    OverQuota,
    /// `[ALREADYEXISTS]`  -  mailbox already exists (e.g. on CREATE) (RFC 5530 Section 3).
    AlreadyExists,
    /// `[NONEXISTENT]`  -  mailbox does not exist (e.g. on SELECT/DELETE) (RFC 5530 Section 3).
    NonExistent,
    /// `[NEWNAME ...]`  -  registered response code, obsolete but still standardized;
    /// trailing data is preserved verbatim (RFC 5530 Section 6).
    NewName(Option<String>),
    /// `[REFERRAL ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    Referral(Option<String>),
    /// `[URLMECH ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    UrlMech(Option<String>),
    /// `[BADURL ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    BadUrl(Option<String>),
    /// `[BADCOMPARATOR ...]`  -  registered response code; trailing data is
    /// preserved verbatim for consumers (RFC 5530 Section 6).
    BadComparator(Option<String>),
    /// `[ANNOTATE ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    Annotate(Option<String>),
    /// `[ANNOTATIONS ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    Annotations(Option<String>),
    /// `[TEMPFAIL ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    TempFail(Option<String>),
    /// `[MAXCONVERTMESSAGES ...]`  -  registered response code; trailing data is
    /// preserved verbatim for consumers (RFC 5530 Section 6).
    MaxConvertMessages(Option<String>),
    /// `[MAXCONVERTPARTS ...]`  -  registered response code; trailing data is
    /// preserved verbatim for consumers (RFC 5530 Section 6).
    MaxConvertParts(Option<String>),
    /// `[NOUPDATE ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5530 Section 6).
    NoUpdate(Option<String>),
    /// `[NOTIFICATIONOVERFLOW ...]`  -  registered response code; trailing data
    /// is preserved verbatim for consumers (RFC 5465 Section 5.8 / RFC 5530 Section 6).
    NotificationOverflow(Option<String>),
    /// `[BADEVENT ...]`  -  registered response code; trailing data is preserved
    /// verbatim for consumers (RFC 5465 Section 5 / RFC 5530 Section 6).
    BadEvent(Option<String>),
    /// `[UNDEFINED-FILTER ...]`  -  registered response code; trailing data is
    /// preserved verbatim for consumers (RFC 5465 Section 8 / RFC 5530 Section 6).
    UndefinedFilter(Option<String>),

    /// `[UIDNOTSTICKY]`  -  assigned UIDs are not persistent (RFC 4315 Section 2 / RFC 9051 Section 7.1).
    UidNotSticky,
    /// `[NOTSAVED]`  -  search result variable `$` is empty (RFC 5182 Section 2.1).
    NotSaved,
    /// `[HASCHILDREN]`  -  mailbox has child mailboxes (RFC 9051 Section 7.1).
    HasChildren,
    /// `[UNKNOWN-CTE]`  -  BINARY fetch failed due to unknown CTE (RFC 3516 Section 4.3).
    UnknownCte,
    /// `[TOOBIG]`  -  message too large for APPEND (RFC 7889 Section 4).
    TooBig,
    /// `[COMPRESSIONACTIVE]`  -  compression layer already active (RFC 4978 Section 3).
    CompressionActive,
    /// `[USEATTR]`  -  special-use attribute not supported (RFC 6154 Section 6).
    UseAttr,

    // --- METADATA (RFC 5464) ---
    /// `[METADATA LONGENTRIES n]`  -  entry values were truncated at `n` bytes
    /// (RFC 5464 Section 4.2.1).
    MetadataLongEntries(u64),
    /// `[METADATA MAXSIZE n]`  -  server's maximum annotation size
    /// (RFC 5464 Section 4.3).
    MetadataMaxSize(u64),
    /// `[METADATA TOOMANY]`  -  too many annotations on this mailbox
    /// (RFC 5464 Section 4.3).
    MetadataTooMany,
    /// `[METADATA NOPRIVATE]`  -  server does not support private annotations
    /// (RFC 5464 Section 4.3).
    MetadataNoPrivate,

    /// Unrecognized response code  -  preserved for forward compatibility.
    Other { name: String, value: Option<String> },
}

/// Server capability (RFC 3501 Section 7.2.1 / RFC 9051 Section 7.2.1).
///
/// Comparison and hashing are case-insensitive per RFC 3501 Section 7.2.1.
#[non_exhaustive]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Capability {
    /// `IMAP4rev1` (RFC 3501).
    Imap4Rev1,
    /// `IMAP4rev2` (RFC 9051).
    Imap4Rev2,
    // --- Extensions (alphabetical) ---
    /// `ACL` (RFC 4314).
    Acl,
    /// `APPENDLIMIT` (RFC 7889 Section 5).
    AppendLimit(Option<u64>),
    /// `BINARY` (RFC 3516).
    Binary,
    /// `CHILDREN` (RFC 3348).
    Children,
    /// `COMPRESS=DEFLATE` (RFC 4978).
    CompressDeflate,
    /// `CONDSTORE` (RFC 7162).
    Condstore,
    /// `CREATE-SPECIAL-USE` (RFC 6154).
    CreateSpecialUse,
    /// `ENABLE` (RFC 5161).
    Enable,
    /// `ESEARCH` (RFC 4731).
    Esearch,
    /// `ID` (RFC 2971).
    Id,
    /// `IDLE` (RFC 2177).
    Idle,
    /// `LIST-EXTENDED` (RFC 5258).
    ListExtended,
    /// `LIST-STATUS` (RFC 5819).
    ListStatus,
    /// `LITERAL+` (RFC 7888).
    LiteralPlus,
    /// `LOGINDISABLED` (RFC 3501 Section 6.2.3 / RFC 9051 Section 6.2.3).
    LoginDisabled,
    /// LITERAL- extension  -  non-synchronizing literals up to 4096 bytes (RFC 7888 Section 5).
    LiteralMinus,
    /// `METADATA` (RFC 5464).
    Metadata,
    /// `METADATA-SERVER`  -  server-only metadata annotations (RFC 5464 Section 1).
    MetadataServer,
    /// `MOVE` (RFC 6851).
    Move,
    /// `MULTIAPPEND` (RFC 3502).
    MultiAppend,
    /// `NAMESPACE` (RFC 2342).
    Namespace,
    /// `NOTIFY` (RFC 5465).
    Notify,
    /// `OBJECTID` (RFC 8474).
    ObjectId,
    /// `QRESYNC` (RFC 7162).
    QResync,
    /// `QUOTA` (RFC 2087).
    Quota,
    /// `QUOTA=RES-<name>`  -  advertised quota resource type (RFC 9208 Section 3.1.1).
    QuotaResource(String),
    /// `QUOTASET`  -  `SETQUOTA` command support (RFC 9208 Section 4.1.3).
    QuotaSet,
    /// `RIGHTS=<chars>`  -  indicates supported ACL rights (RFC 4314 Section 6).
    ///
    /// The `String` holds the new-rights characters (e.g. `"texk"`).
    Rights(String),
    /// `PREVIEW` (RFC 8970 Section 4).
    Preview,
    /// `SASL-IR` (RFC 4959).
    SaslIr,
    /// `SAVEDATE` (RFC 8514).
    SaveDate,
    /// `SEARCHRES` (RFC 5182).
    SearchRes,
    /// SORT extension (RFC 5256 Section 1).
    Sort,
    /// `SORT=DISPLAY` extension (RFC 5957).
    SortDisplay(String),
    /// `STARTTLS` (RFC 3501 Section 6.2.1 / RFC 9051 Section 6.2.1).
    StartTls,
    /// `SPECIAL-USE` (RFC 6154).
    SpecialUse,
    /// `THREAD=<algorithm>` (RFC 5256 Section 1).
    ///
    /// The String holds the algorithm name (e.g. `"REFERENCES"`, `"ORDEREDSUBJECT"`).
    /// Servers may advertise multiple `THREAD=` capabilities, each as a separate entry.
    Thread(String),
    /// `STATUS=SIZE` (RFC 8438).
    StatusSize,
    /// `UIDPLUS` (RFC 4315).
    UidPlus,
    /// `UNAUTHENTICATE` (RFC 8437 Section 2).
    Unauthenticate,
    /// `UNSELECT` (RFC 3691).
    Unselect,
    /// `UTF8=ACCEPT` (RFC 6855).
    Utf8Accept,
    /// `UTF8=ONLY` (RFC 6855 Section 4).
    Utf8Only,
    /// `WITHIN` (RFC 5032 Section 3).
    ///
    /// Enables OLDER and YOUNGER search keys for time-relative searches.
    Within,
    /// Gmail extension capability for `X-GM-*` FETCH attributes.
    XGmExt1,
    /// `AUTH=<mechanism>` (e.g. `AUTH=PLAIN`, `AUTH=XOAUTH2`) (RFC 3501 Section 7.2.1).
    Auth(String),
    /// Unrecognized capability  -  preserved verbatim.
    Other(String),
}

impl Capability {
    /// Returns the wire representation of this capability
    /// (e.g. `IDLE`, `AUTH=PLAIN`, `THREAD=REFERENCES`)
    /// (RFC 3501 Section 7.2.1 / RFC 9051 Section 7.2.1).
    pub fn as_imap_str(&self) -> String {
        match self {
            Self::Imap4Rev1 => "IMAP4rev1".into(),
            Self::Imap4Rev2 => "IMAP4rev2".into(),
            Self::Acl => "ACL".into(),
            Self::AppendLimit(Some(n)) => format!("APPENDLIMIT={n}"),
            Self::AppendLimit(None) => "APPENDLIMIT".into(),
            Self::Binary => "BINARY".into(),
            Self::Children => "CHILDREN".into(),
            Self::CompressDeflate => "COMPRESS=DEFLATE".into(),
            Self::Condstore => "CONDSTORE".into(),
            Self::CreateSpecialUse => "CREATE-SPECIAL-USE".into(),
            Self::Enable => "ENABLE".into(),
            Self::Esearch => "ESEARCH".into(),
            Self::Id => "ID".into(),
            Self::Idle => "IDLE".into(),
            Self::ListExtended => "LIST-EXTENDED".into(),
            Self::ListStatus => "LIST-STATUS".into(),
            Self::LiteralPlus => "LITERAL+".into(),
            Self::LoginDisabled => "LOGINDISABLED".into(),
            Self::LiteralMinus => "LITERAL-".into(),
            Self::Metadata => "METADATA".into(),
            Self::MetadataServer => "METADATA-SERVER".into(),
            Self::Move => "MOVE".into(),
            Self::MultiAppend => "MULTIAPPEND".into(),
            Self::Namespace => "NAMESPACE".into(),
            Self::Notify => "NOTIFY".into(),
            Self::ObjectId => "OBJECTID".into(),
            Self::Preview => "PREVIEW".into(),
            Self::QResync => "QRESYNC".into(),
            Self::Quota => "QUOTA".into(),
            Self::QuotaResource(s) => format!("QUOTA=RES-{s}"),
            Self::QuotaSet => "QUOTASET".into(),
            Self::Rights(s) => format!("RIGHTS={s}"),
            Self::SaslIr => "SASL-IR".into(),
            Self::SaveDate => "SAVEDATE".into(),
            Self::SearchRes => "SEARCHRES".into(),
            Self::Sort => "SORT".into(),
            Self::SortDisplay(s) => format!("SORT={s}"),
            Self::StartTls => "STARTTLS".into(),
            Self::SpecialUse => "SPECIAL-USE".into(),
            Self::Thread(s) => format!("THREAD={s}"),
            Self::StatusSize => "STATUS=SIZE".into(),
            Self::Unauthenticate => "UNAUTHENTICATE".into(),
            Self::UidPlus => "UIDPLUS".into(),
            Self::Unselect => "UNSELECT".into(),
            Self::Within => "WITHIN".into(),
            Self::XGmExt1 => "X-GM-EXT-1".into(),
            Self::Utf8Accept => "UTF8=ACCEPT".into(),
            Self::Utf8Only => "UTF8=ONLY".into(),
            Self::Auth(s) => format!("AUTH={s}"),
            Self::Other(s) => s.clone(),
        }
    }

    /// Parse a capability token from its IMAP wire representation
    /// (RFC 3501 Section 7.2.1 / RFC 9051 Section 7.2.2).
    ///
    /// Case-insensitive per RFC 3501 Section 7.2.1: "Strstrings in capability
    /// names are case-insensitive."
    #[allow(clippy::too_many_lines)]
    pub fn from_imap_str(s: &str) -> Self {
        let upper = s.to_ascii_uppercase();
        match upper.as_str() {
            "IMAP4REV1" => Self::Imap4Rev1,
            "IMAP4REV2" => Self::Imap4Rev2,
            "ACL" => Self::Acl,
            "BINARY" => Self::Binary,
            "CHILDREN" => Self::Children,
            "COMPRESS=DEFLATE" => Self::CompressDeflate,
            "CONDSTORE" => Self::Condstore,
            "CREATE-SPECIAL-USE" => Self::CreateSpecialUse,
            "ENABLE" => Self::Enable,
            "ESEARCH" => Self::Esearch,
            "ID" => Self::Id,
            "IDLE" => Self::Idle,
            "LIST-EXTENDED" => Self::ListExtended,
            "LIST-STATUS" => Self::ListStatus,
            "LITERAL+" => Self::LiteralPlus,
            "LITERAL-" => Self::LiteralMinus,
            "LOGINDISABLED" => Self::LoginDisabled,
            "METADATA" => Self::Metadata,
            "METADATA-SERVER" => Self::MetadataServer,
            "MOVE" => Self::Move,
            "MULTIAPPEND" => Self::MultiAppend,
            "NAMESPACE" => Self::Namespace,
            "NOTIFY" => Self::Notify,
            "OBJECTID" => Self::ObjectId,
            "PREVIEW" => Self::Preview,
            "QRESYNC" => Self::QResync,
            "QUOTA" => Self::Quota,
            "QUOTASET" => Self::QuotaSet,
            "SASL-IR" => Self::SaslIr,
            "SAVEDATE" => Self::SaveDate,
            "SEARCHRES" => Self::SearchRes,
            "SORT" => Self::Sort,
            "SPECIAL-USE" => Self::SpecialUse,
            "STARTTLS" => Self::StartTls,
            "STATUS=SIZE" => Self::StatusSize,
            // RFC 8437 Section 2: UNAUTHENTICATE command support.
            "UNAUTHENTICATE" => Self::Unauthenticate,
            "UIDPLUS" => Self::UidPlus,
            "UNSELECT" => Self::Unselect,
            // RFC 5032 Section 3: WITHIN enables OLDER/YOUNGER search keys.
            "WITHIN" => Self::Within,
            "X-GM-EXT-1" => Self::XGmExt1,
            "UTF8=ACCEPT" => Self::Utf8Accept,
            "UTF8=ONLY" => Self::Utf8Only,
            _ => {
                if let Some(mechanism) = upper.strip_prefix("AUTH=") {
                    // RFC 3501 Section 9 / RFC 9051 Section 9:
                    // capability = ("AUTH=" auth-type) / atom
                    // auth-type = atom, so an empty suffix is malformed.
                    if mechanism.is_empty() {
                        Self::Other(s.to_owned())
                    } else {
                        Self::Auth(mechanism.to_owned())
                    }
                } else if upper == "SORT=DISPLAY" {
                    // RFC 5957 defines the dedicated SORT=DISPLAY capability.
                    Self::SortDisplay("DISPLAY".to_owned())
                } else if let Some(resource) = upper.strip_prefix("QUOTA=RES-") {
                    // RFC 9208 Section 3.1.1: supported quota resources are
                    // advertised as `QUOTA=RES-<name>`, so the suffix is required.
                    if resource.is_empty() {
                        Self::Other(s.to_owned())
                    } else {
                        Self::QuotaResource(s["QUOTA=RES-".len()..].to_string())
                    }
                } else if let Some(algo) = upper.strip_prefix("THREAD=") {
                    // THREAD=REFERENCES, THREAD=ORDEREDSUBJECT, etc. (RFC 5256 Section 1)
                    if algo.is_empty() {
                        Self::Other(s.to_owned())
                    } else {
                        Self::Thread(algo.to_owned())
                    }
                } else if let Some(rights) = upper.strip_prefix("RIGHTS=") {
                    // RIGHTS=<chars> (RFC 4314 Section 6)
                    // Preserve the original case of the rights characters.
                    // RFC 4314 Section 7: rights-capa = "RIGHTS=" new-rights,
                    // and new-rights = 1*LOWER-ALPHA, so an empty suffix is malformed.
                    if rights.is_empty() {
                        Self::Other(s.to_owned())
                    } else {
                        Self::Rights(s["RIGHTS=".len()..].to_string())
                    }
                } else if let Some(rest) = upper.strip_prefix("APPENDLIMIT") {
                    if rest.is_empty() {
                        // Bare `APPENDLIMIT` with no `=value` means server-wide limit
                        // must be checked per-mailbox (RFC 7889 Section 2).
                        Self::AppendLimit(None)
                    } else if let Some(val_str) = rest.strip_prefix('=') {
                        if !val_str.is_empty() && val_str.bytes().all(|b| b.is_ascii_digit()) {
                            if let Ok(n) = val_str.parse::<u64>() {
                                Self::AppendLimit(Some(n))
                            } else {
                                // Non-numeric  -  preserve as-is.
                                Self::Other(s.to_owned())
                            }
                        } else {
                            // Non-numeric  -  preserve as-is.
                            Self::Other(s.to_owned())
                        }
                    } else {
                        // RFC 7889 Section 5 only defines bare `APPENDLIMIT`
                        // and `APPENDLIMIT=<number>`. Any other token starting
                        // with that prefix is an unknown capability and must be
                        // preserved verbatim for forward compatibility.
                        Self::Other(s.to_owned())
                    }
                } else {
                    Self::Other(s.to_owned())
                }
            }
        }
    }
}

/// Converts a string to a `Capability` using case-insensitive matching
/// (RFC 3501 Section 7.2.1).
impl From<String> for Capability {
    fn from(s: String) -> Self {
        Self::from_imap_str(&s)
    }
}

/// Converts a string slice to a `Capability` using case-insensitive matching
/// (RFC 3501 Section 7.2.1).
impl From<&str> for Capability {
    fn from(s: &str) -> Self {
        Self::from_imap_str(s)
    }
}

/// RFC 3501 Section 7.2.1: "There is no requirement that capability names be
/// registered"  -  capability names are atoms and IMAP atoms are case-insensitive.
///
/// Known capability variants with no string payload compare by discriminant.
/// String-carrying variants (`Auth`, `Thread`, `SortDisplay`, `Rights`, `Other`)
/// compare using ASCII case-insensitive comparison so that e.g.
/// `Auth("PLAIN")` and `Auth("plain")` are treated as the same capability.
///
/// Cross-representation is also handled: `Other("IDLE")` equals `Idle`,
/// because they denote the same protocol capability.
impl PartialEq for Capability {
    fn eq(&self, other: &Self) -> bool {
        // RFC 3501 Section 7.2.1: capability comparisons are case-insensitive.
        match (self, other) {
            (Self::Imap4Rev1, Self::Imap4Rev1)
            | (Self::Imap4Rev2, Self::Imap4Rev2)
            | (Self::Acl, Self::Acl)
            | (Self::Binary, Self::Binary)
            | (Self::Children, Self::Children)
            | (Self::CompressDeflate, Self::CompressDeflate)
            | (Self::Condstore, Self::Condstore)
            | (Self::CreateSpecialUse, Self::CreateSpecialUse)
            | (Self::Enable, Self::Enable)
            | (Self::Esearch, Self::Esearch)
            | (Self::Id, Self::Id)
            | (Self::Idle, Self::Idle)
            | (Self::ListExtended, Self::ListExtended)
            | (Self::ListStatus, Self::ListStatus)
            | (Self::LiteralPlus, Self::LiteralPlus)
            | (Self::LoginDisabled, Self::LoginDisabled)
            | (Self::LiteralMinus, Self::LiteralMinus)
            | (Self::Metadata, Self::Metadata)
            | (Self::MetadataServer, Self::MetadataServer)
            | (Self::Move, Self::Move)
            | (Self::MultiAppend, Self::MultiAppend)
            | (Self::Namespace, Self::Namespace)
            | (Self::Notify, Self::Notify)
            | (Self::ObjectId, Self::ObjectId)
            | (Self::Preview, Self::Preview)
            | (Self::QResync, Self::QResync)
            | (Self::Quota, Self::Quota)
            | (Self::QuotaSet, Self::QuotaSet)
            | (Self::SaslIr, Self::SaslIr)
            | (Self::SaveDate, Self::SaveDate)
            | (Self::SearchRes, Self::SearchRes)
            | (Self::Sort, Self::Sort)
            | (Self::StartTls, Self::StartTls)
            | (Self::SpecialUse, Self::SpecialUse)
            | (Self::StatusSize, Self::StatusSize)
            | (Self::Unauthenticate, Self::Unauthenticate)
            | (Self::UidPlus, Self::UidPlus)
            | (Self::Unselect, Self::Unselect)
            | (Self::Within, Self::Within)
            | (Self::XGmExt1, Self::XGmExt1)
            | (Self::Utf8Accept, Self::Utf8Accept)
            | (Self::Utf8Only, Self::Utf8Only) => true,
            (Self::AppendLimit(a), Self::AppendLimit(b)) => a == b,
            (Self::Auth(a), Self::Auth(b))
            | (Self::Thread(a), Self::Thread(b))
            | (Self::SortDisplay(a), Self::SortDisplay(b))
            | (Self::QuotaResource(a), Self::QuotaResource(b))
            | (Self::Rights(a), Self::Rights(b))
            | (Self::Other(a), Self::Other(b)) => a.eq_ignore_ascii_case(b),
            // Cross-representation: compare Other's wire form against known variant.
            (Self::Other(s), known) | (known, Self::Other(s)) => {
                s.eq_ignore_ascii_case(&known.as_imap_str())
            }
            _ => false,
        }
    }
}

/// RFC 3501 Section 7.2.1: capability equality is reflexive, symmetric, transitive.
impl Eq for Capability {}

/// RFC 3501 Section 7.2.1: capability names are case-insensitive.
///
/// The `Hash` implementation must be consistent with `PartialEq`: capabilities that
/// compare equal must hash to the same value. Because `Other("IDLE")` must
/// equal `Idle`, we hash the lowercased wire form (`as_imap_str()`) for all
/// variants, which is identical for cross-representation equivalents.
impl std::hash::Hash for Capability {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // RFC 3501 Section 7.2.1: case-insensitive hashing via wire form.
        // Other("IDLE") and Idle both yield "IDLE", so lowercasing
        // produces the same hash.
        for byte in self.as_imap_str().as_bytes() {
            byte.to_ascii_lowercase().hash(state);
        }
    }
}

impl std::fmt::Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.as_imap_str(), f)
    }
}

/// A single namespace entry from a NAMESPACE response (RFC 2342).
///
/// RFC 2342 Section 6 ABNF:
/// ```text
/// Namespace = nil / "(" 1*( "(" string SP  (<"> QUOTED_CHAR <"> / nil)
///                    *(Namespace_Response_Extension) ")" ) ")"
/// Namespace_Response_Extension = SP string SP "(" string *(SP string) ")"
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NamespaceDescriptor {
    /// Namespace prefix (e.g. `""`, `"INBOX."`, `"#shared."`) (RFC 2342 Section 5).
    pub prefix: String,
    /// Hierarchy delimiter for this namespace, or `None` if flat (RFC 2342 Section 5).
    pub delimiter: Option<char>,
    /// Extension key-value-list pairs (RFC 2342 Section 6).
    ///
    /// Each entry is `(key, values)` where key is a string and values is a
    /// non-empty list of strings, corresponding to one
    /// `Namespace_Response_Extension = SP string SP "(" string *(SP string) ")"`.
    pub extensions: Vec<(String, Vec<String>)>,
}

/// Result of a NAMESPACE command (RFC 2342 Section 5).
///
/// Groups the three namespace categories into named fields instead of a
/// positional tuple, making it safe to extend in the future.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NamespaceResponse {
    /// Personal namespaces (RFC 2342 Section 5).
    pub personal: Vec<NamespaceDescriptor>,
    /// Other users' namespaces (RFC 2342 Section 5).
    pub other: Vec<NamespaceDescriptor>,
    /// Shared namespaces (RFC 2342 Section 5).
    pub shared: Vec<NamespaceDescriptor>,
}

/// Result of a GETQUOTAROOT command (RFC 2087 Section 4.3 / RFC 9208 Section 4.1.2).
///
/// Contains the quota root names and the quota resources associated with
/// each root.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct QuotaRootResponse {
    /// Quota root names returned by the server (RFC 2087 Section 4.3).
    pub roots: Vec<String>,
    /// Quota resources keyed by root name (RFC 2087 Section 4.3).
    ///
    /// Each entry is `(root_name, resources)` where `resources` is the list
    /// of resource triplets for that root.
    pub resources: Vec<(String, Vec<QuotaResource>)>,
}

/// Result of a LISTRIGHTS command (RFC 4314 Section 3.4).
///
/// Contains the rights that are always granted and the groups of optional
/// rights that can be independently granted.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ListRightsResponse {
    /// Rights that are always granted to the identifier (RFC 4314 Section 3.4).
    pub required: String,
    /// Groups of optional rights that can be independently granted
    /// (RFC 4314 Section 3.4).
    ///
    /// Each element is a string of right characters that form an indivisible
    /// group: granting any character in the group grants all of them.
    pub optional: Vec<String>,
}

/// ESEARCH response data (RFC 4731 Section 3.1).
///
/// RFC 4731 Section 3.1 ABNF:
/// `search-return-data = "MIN" SP nz-number / "MAX" SP nz-number /
///                        "ALL" SP sequence-set / "COUNT" SP number`
///
/// Normative rules:
/// - MIN: "Return the lowest message number/UID that satisfies the SEARCH criteria.
///   If the SEARCH results in no matches, the server MUST NOT include the MIN result
///   option in the ESEARCH response."
/// - MAX: "Return the highest message number/UID that satisfies the SEARCH criteria.
///   If the SEARCH results in no matches, the server MUST NOT include the MAX result
///   option in the ESEARCH response."
/// - ALL: Returns matching messages as a sequence-set rather than space-separated.
///   "If the SEARCH results in no matches, the server MUST NOT include the ALL result
///   option in the ESEARCH response."
/// - COUNT: "Return number of the messages that satisfy the SEARCH criteria. This result
///   option MUST always be included in the ESEARCH response."
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EsearchResponse {
    /// Correlating tag from `(TAG "tagstring")`, if present
    /// (RFC 4466 Section 2.6.2 `search-correlator`).
    pub tag: Option<String>,
    /// `true` when the response includes the `UID` indicator,
    /// meaning all returned numbers are UIDs rather than sequence numbers
    /// (RFC 4731 Section 3.1).
    pub uid: bool,
    /// MIN  -  lowest matching message number/UID (RFC 4731 Section 3.1).
    pub min: Option<u32>,
    /// MAX  -  highest matching message number/UID (RFC 4731 Section 3.1).
    pub max: Option<u32>,
    /// COUNT  -  number of matching messages (RFC 4731 Section 3.1).
    pub count: Option<u32>,
    /// ALL  -  matching message numbers/UIDs as a uid-set (RFC 4731 Section 3.1).
    pub all: Vec<UidRange>,
    /// MODSEQ  -  highest mod-sequence of matching messages (RFC 7162 Section 3.1.10).
    pub mod_seq: Option<u64>,
}

/// A UID range (e.g. `1:100`, or a single UID `42`)
/// (RFC 3501 Section 9 / RFC 4315 Section 2.1).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UidRange {
    /// First UID in this range (RFC 3501 Section 9 / RFC 4315 Section 2.1).
    pub start: u32,
    /// `None` means a single UID (not a range) (RFC 3501 Section 9 / RFC 4315 Section 2.1).
    pub end: Option<u32>,
}

impl UidRange {
    /// Create a single-UID range.
    ///
    /// # Panics (debug builds only)
    /// Panics if `uid` is 0  -  UIDs are `nz-number` per RFC 3501 Section 9.
    pub const fn single(uid: u32) -> Self {
        debug_assert!(
            uid != 0,
            "UID must be non-zero (RFC 3501 Section 9: uniqueid = nz-number)"
        );
        Self {
            start: uid,
            end: None,
        }
    }

    /// Create an inclusive UID range.
    ///
    /// # Panics (debug builds only)
    /// Panics if `start` or `end` is 0  -  UIDs are `nz-number` per RFC 3501 Section 9.
    pub const fn range(start: u32, end: u32) -> Self {
        debug_assert!(
            start != 0,
            "UID start must be non-zero (RFC 3501 Section 9: uniqueid = nz-number)"
        );
        debug_assert!(
            end != 0,
            "UID end must be non-zero (RFC 3501 Section 9: uniqueid = nz-number)"
        );
        Self {
            start,
            end: Some(end),
        }
    }

    /// Try to create a single-UID range, returning `None` if `uid` is 0
    /// (RFC 3501 Section 9: uniqueid = nz-number).
    pub const fn try_single(uid: u32) -> Option<Self> {
        if uid == 0 {
            None
        } else {
            Some(Self {
                start: uid,
                end: None,
            })
        }
    }

    /// Try to create an inclusive UID range, returning `None` if `start` or `end` is 0
    /// (RFC 3501 Section 9: uniqueid = nz-number).
    pub const fn try_range(start: u32, end: u32) -> Option<Self> {
        if start == 0 || end == 0 {
            None
        } else {
            Some(Self {
                start,
                end: Some(end),
            })
        }
    }
}

/// Result of an EXPUNGE command (RFC 3501 Section 7.4.1 / RFC 7162 Section 3.2.10).
///
/// When QRESYNC is enabled (RFC 7162 Section 3.2.3), the server sends
/// `VANISHED` responses instead of `EXPUNGE`. This enum allows callers
/// to handle both cases.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ExpungeResult {
    /// Classic EXPUNGE  -  sequence numbers of removed messages (RFC 3501 Section 7.4.1).
    ///
    /// Returned when QRESYNC is NOT enabled.
    Expunged(Vec<u32>),
    /// VANISHED  -  UID ranges of removed messages (RFC 7162 Section 3.2.10).
    ///
    /// Returned when QRESYNC IS enabled. The server sends VANISHED
    /// instead of EXPUNGE after `ENABLE QRESYNC`.
    Vanished(Vec<UidRange>),
}

impl Default for ExpungeResult {
    fn default() -> Self {
        Self::Expunged(Vec::new())
    }
}

/// Result of a MOVE command (RFC 6851 Section 3).
///
/// RFC 6851 Section 3 specifies that the server sends EXPUNGE (or VANISHED
/// when QRESYNC is enabled per RFC 7162 Section 3.2.10) responses *before*
/// the tagged OK, followed by a COPYUID response code in the tagged OK
/// (RFC 6851 Section 4.3). This struct captures both pieces of information.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MoveResult {
    /// The response code from the tagged OK, typically `COPYUID`
    /// (RFC 6851 Section 4.3).
    pub code: Option<ResponseCode>,
    /// The EXPUNGE or VANISHED responses that preceded the tagged OK
    /// (RFC 6851 Section 3 / RFC 7162 Section 3.2.10).
    pub expunged: ExpungeResult,
}

/// Result of a COPY or UID COPY command (RFC 3501 Section 6.4.7).
///
/// RFC 4315 Section 3 specifies that the server SHOULD respond with a
/// `[COPYUID uid-validity source-uids dest-uids]` response code in the
/// tagged OK. This struct captures that response code.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CopyResult {
    /// The response code from the tagged OK, typically `COPYUID`
    /// (RFC 4315 Section 3). `None` when the server omits the response code.
    pub code: Option<ResponseCode>,
}

/// Parameters for QRESYNC-enabled SELECT/EXAMINE (RFC 7162 Section 3.2.5.2).
///
/// Allows the client to provide its last known state so the server can send
/// only the changes since the last synchronization point.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct QresyncParams {
    /// The UIDVALIDITY value from the last session (RFC 7162 Section 3.2.5.2).
    pub uid_validity: u32,
    /// The highest MODSEQ value the client has cached (RFC 7162 Section 3.2.5.2).
    pub mod_seq: u64,
    /// Optional set of known UIDs for more efficient resync (RFC 7162 Section 3.2.5.2).
    pub known_uids: Option<String>,
    /// Optional sequence-to-UID mapping for detecting message renumbering
    /// (RFC 7162 Section 3.2.5.2).
    ///
    /// `seq-match-data = "(" known-sequence-set SP known-uid-set ")"`
    pub seq_match_data: Option<(String, String)>,
}

impl QresyncParams {
    /// Create QRESYNC parameters with the required fields
    /// (RFC 7162 Section 3.2.5.2).
    ///
    /// Optional fields (`known_uids`, `seq_match_data`) default to `None`.
    pub fn new(uid_validity: u32, mod_seq: u64) -> Self {
        Self {
            uid_validity,
            mod_seq,
            known_uids: None,
            seq_match_data: None,
        }
    }
}

/// Options for SELECT/EXAMINE commands beyond the basic mailbox name
/// (RFC 3501 Sections 6.3.1/6.3.2, RFC 7162 Sections 3.1.8 and 3.2.5.2).
///
/// Used with [`ImapConnection::select_with`] and [`ImapConnection::examine_with`]
/// to request CONDSTORE or QRESYNC extensions without requiring separate method
/// variants for each combination.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SelectOptions {
    /// Enable CONDSTORE per-message mod-sequence tracking
    /// (RFC 7162 Section 3.1.1).
    ///
    /// When `true`, the server includes `HIGHESTMODSEQ` in the OK response
    /// and tracks per-message MODSEQ values for the selected mailbox.
    /// Requires the CONDSTORE or QRESYNC capability (RFC 7162 Section 3.1).
    pub condstore: bool,
    /// QRESYNC parameters for efficient delta sync
    /// (RFC 7162 Section 3.2.5.2).
    ///
    /// Provides the server with the client's last known UIDVALIDITY and MODSEQ
    /// so it can send `VANISHED (EARLIER)` and `FETCH (FLAGS)` for changed
    /// messages instead of a full resync.
    /// Requires the QRESYNC capability to have been enabled first
    /// (RFC 7162 Section 3.2.5).
    pub qresync: Option<QresyncParams>,
}

impl SelectOptions {
    /// Create options with CONDSTORE enabled (RFC 7162 Section 3.1.1).
    pub fn condstore() -> Self {
        Self {
            condstore: true,
            ..Self::default()
        }
    }

    /// Create options with QRESYNC parameters (RFC 7162 Section 3.2.5.2).
    pub fn qresync(params: QresyncParams) -> Self {
        Self {
            qresync: Some(params),
            ..Self::default()
        }
    }
}

/// A single quota resource from a QUOTA response (RFC 2087 Section 5.1).
///
/// Each resource triplet consists of a name (e.g. `STORAGE`, `MESSAGE`),
/// the current usage, and the limit.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct QuotaResource {
    /// Resource name (e.g. `"STORAGE"`, `"MESSAGE"`) (RFC 2087 Section 5.1).
    pub name: String,
    /// Current usage of this resource (RFC 2087 Section 5.1).
    pub usage: u64,
    /// Server-imposed limit for this resource (RFC 2087 Section 5.1).
    pub limit: u64,
}

/// A single ACL entry from an ACL response (RFC 4314 Section 3.6).
///
/// Each entry pairs an identifier (user or group name) with a rights string.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AclEntry {
    /// The identifier (user or group) this entry applies to (RFC 4314 Section 3.6).
    pub identifier: String,
    /// The rights string for this identifier (RFC 4314 Section 3.6).
    pub rights: String,
}

/// A single metadata entry from a METADATA response (RFC 5464 Section 4.4).
///
/// Each entry has a name (e.g. `/private/comment`) and an optional value.
/// A `None` value indicates the entry does not exist or has been deleted.
///
/// RFC 5464 Section 5 formal syntax: `value = nstring / literal8`.
/// The `literal8` form (`~{n}\r\n<data>`) allows arbitrary binary octets,
/// so the value is stored as raw bytes rather than a UTF-8 string.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MetadataEntry {
    /// Entry name (e.g. `/private/comment`, `/shared/vendor/foo`) (RFC 5464 Section 3.2).
    pub name: String,
    /// Entry value as raw bytes, or `None` if the entry does not exist (RFC 5464 Section 4.4).
    ///
    /// RFC 5464 Section 5: `value = nstring / literal8`  -  values may contain
    /// arbitrary binary data via the `literal8` syntax, so `Vec<u8>` is used
    /// instead of `String` to preserve binary fidelity.
    pub value: Option<Vec<u8>>,
}

/// Result of a `GETMETADATA` command (RFC 5464 Section 4.2).
///
/// When NOTIFY metadata is active (RFC 5465 Sections 5.6-5.8), the protocol
/// provides no marker to distinguish solicited `METADATA` responses from
/// unsolicited NOTIFY `METADATA` for the same mailbox  -  they are
/// wire-identical.  Unlike `STATUS` (which expects exactly one solicited
/// response, enabling a last-match heuristic), `GETMETADATA` can legitimately
/// produce multiple solicited `METADATA` response lines, so no reliable
/// heuristic exists.
///
/// This struct exposes the ambiguity via [`notify_ambiguity`](Self::notify_ambiguity)
/// so callers can take appropriate action (e.g. treat entries as potentially
/// stale, re-query without NOTIFY, or duplicate entries to a NOTIFY event
/// pipeline).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MetadataResult {
    /// All metadata entries from matching `METADATA` responses, merged.
    pub entries: Vec<MetadataEntry>,
    /// `true` when NOTIFY metadata was active during the call (RFC 5465
    /// Sections 5.6-5.8), meaning some entries may be from unsolicited
    /// NOTIFY events that were indistinguishable from the solicited response.
    ///
    /// When `true`, callers that require unambiguous results should avoid
    /// issuing `GETMETADATA` while NOTIFY metadata delivery is active, or
    /// treat all returned entries as potentially including interleaved
    /// notifications.
    pub notify_ambiguity: bool,
}

/// A node in a THREAD response tree (RFC 5256 Section 4).
///
/// Each node represents a message in a thread. A dummy parent (`id == None`)
/// is used when the threading algorithm infers a parent that does not
/// correspond to an existing message.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ThreadNode {
    /// UID (or sequence number) of this message, or `None` if this is a
    /// dummy parent (RFC 5256 Section 4).
    pub id: Option<u32>,
    /// Child thread nodes (RFC 5256 Section 4).
    pub children: Vec<Self>,
}

#[cfg(test)]
#[path = "response_tests.rs"]
mod tests;
