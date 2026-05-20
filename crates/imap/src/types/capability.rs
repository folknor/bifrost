/// Server capability (RFC 3501 Section 7.2.1 / RFC 9051 Section 7.2.1).
///
/// Comparison and hashing are case-insensitive per RFC 3501 Section 7.2.1.
#[non_exhaustive]
#[derive(Debug, Clone)]
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
    /// `STATUS=DELETED` (RFC 9051 Section 6.3.11).
    StatusDeleted,
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
            Self::StatusDeleted => "STATUS=DELETED".into(),
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
            "STATUS=DELETED" => Self::StatusDeleted,
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
            | (Self::StatusDeleted, Self::StatusDeleted)
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
