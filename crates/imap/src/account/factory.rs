use std::sync::Arc;
use std::time::Duration;

use bifrost_types::{Account, AccountError, AccountFactory, AccountFuture, AccountId};

use bifrost_caldav::{CalDavAccountFactory, CalDavConfig};
use bifrost_carddav::{CardDavAccountFactory, CardDavConfig};

use crate::connection::ImapConfig;
use crate::types::{
    AuthPolicy, Capability, Credentials, MailboxInfo, MailboxRights, ServerProfile,
};

use super::error::ImapErrorContext;
use super::sieve::ManageSieveConfig;
use super::submission::{SmtpSubmissionConfig, SubmissionTransport};
use super::{
    ImapAccount, ImapAccountParts, Pool, account_error_with, capabilities,
    folder_registry::FolderRegistry,
};
use bifrost_types::AccountOperation;

/// Account-boundary translation for every leg of `open` (connect,
/// AUTH, ID, QRESYNC, LIST). All four are tagged `Discover` because
/// they belong to a single discovery phase. The AUTH leg is genuinely
/// idempotent at the protocol level (re-authenticating produces the
/// same outcome), so an `InFlight` transport drop here is safe to
/// retry; the central recovery mapping picks `Retry::SameRequest` for
/// idempotent ops regardless. Splitting these into per-phase variants
/// would not change recovery for IMAP; it would only inflate the
/// `AccountOperation` enum surface.
fn discover_err(err: crate::Error) -> AccountError {
    account_error_with(err, ImapErrorContext::operation(AccountOperation::Discover))
}

/// Configuration used by `ImapAccountFactory`.
#[non_exhaustive]
#[derive(Clone)]
pub struct ImapAccountConfig {
    pub imap: ImapConfig,
    pub credentials: Credentials,
    pub auth_policy: AuthPolicy,
    pub pool_cap: usize,
    pub idle_timeout: Duration,
    pub enable_qresync: bool,
    pub flag_sync_interval: Duration,
    pub deletion_check_interval: Duration,
    pub mutation_batch_size: usize,
    pub bandwidth_meter: Option<Arc<bifrost_net::BandwidthMeter>>,
    pub meter_sink: Option<Arc<dyn bifrost_net::MeterSink>>,
    pub sieve: Option<ManageSieveConfig>,
    pub carddav: Option<CardDavConfig>,
    pub caldav: Option<CalDavConfig>,
    pub submission: Option<SmtpSubmissionConfig>,
}

impl ImapAccountConfig {
    pub fn new(imap: ImapConfig, credentials: Credentials, auth_policy: AuthPolicy) -> Self {
        Self {
            imap,
            credentials,
            auth_policy,
            pool_cap: 4,
            idle_timeout: Duration::from_secs(29 * 60),
            enable_qresync: false,
            flag_sync_interval: Duration::from_secs(300),
            deletion_check_interval: Duration::from_secs(600),
            mutation_batch_size: 1024,
            bandwidth_meter: None,
            meter_sink: None,
            sieve: None,
            carddav: None,
            caldav: None,
            submission: None,
        }
    }

    pub fn with_bandwidth_meter(mut self, meter: Arc<bifrost_net::BandwidthMeter>) -> Self {
        self.bandwidth_meter = Some(meter);
        self
    }

    pub fn with_meter_sink(mut self, sink: Arc<dyn bifrost_net::MeterSink>) -> Self {
        self.meter_sink = Some(sink);
        self
    }

    pub fn with_manage_sieve(mut self, config: ManageSieveConfig) -> Self {
        self.sieve = Some(config);
        self
    }

    pub fn with_carddav(mut self, config: CardDavConfig) -> Self {
        self.carddav = Some(config);
        self
    }

    pub fn with_caldav(mut self, config: CalDavConfig) -> Self {
        self.caldav = Some(config);
        self
    }

    pub fn with_submission(mut self, config: SmtpSubmissionConfig) -> Self {
        self.submission = Some(config);
        self
    }
}

pub struct ImapAccountFactory {
    cfg: Arc<ImapAccountConfig>,
}

impl ImapAccountFactory {
    pub fn new(config: ImapAccountConfig) -> Self {
        Self {
            cfg: Arc::new(config),
        }
    }
}

impl AccountFactory for ImapAccountFactory {
    fn open(&self, account_id: AccountId) -> AccountFuture<Result<Arc<dyn Account>, AccountError>> {
        let cfg = Arc::clone(&self.cfg);
        Box::pin(async move {
            let bandwidth_cap = Arc::new(std::sync::atomic::AtomicU64::new(
                super::UNLIMITED_BANDWIDTH,
            ));
            let meter = meter_handle(&cfg, account_id.clone());
            let (conn, _auth) = cfg
                .imap
                .connect_authenticated_metered(
                    &cfg.credentials,
                    &cfg.auth_policy,
                    meter.clone(),
                    Some(Arc::clone(&bandwidth_cap)),
                )
                .await
                .map_err(discover_err)?;

            let mut profile = conn.server_profile();
            let server_id = read_server_id(&conn, &cfg, &profile)
                .await
                .map_err(discover_err)?;
            let qresync = negotiate_qresync(&conn, &cfg, &profile, &server_id)
                .await
                .map_err(discover_err)?;
            profile = conn.server_profile();

            let folders = list_folders(&conn, &cfg, &profile)
                .await
                .map_err(discover_err)?;
            // Surface shared/other-user folders via NAMESPACE + ACL gating
            // (A5c). A NAMESPACE-less or personal-only server yields an
            // empty list; a single revoked prefix is skipped, not fatal.
            let shared = discover_shared_folders(&conn, &cfg, &profile)
                .await
                .into_iter()
                .map(|sf| (sf.info, sf.owner))
                .collect::<Vec<_>>();
            // Fail-soft: a DAV open failure degrades to IMAP-only for
            // this cycle instead of failing the whole IMAP account.
            let mut dav_degraded = Vec::new();
            let contacts = open_carddav(&cfg, account_id.clone())
                .await
                .into_attached(&mut dav_degraded);
            let calendars = open_caldav(&cfg, account_id.clone())
                .await
                .into_attached(&mut dav_degraded);
            let submission = open_submission(&cfg)?;
            let caps = capabilities::build_capabilities(
                &profile,
                &folders,
                cfg.sieve.is_some(),
                contacts.as_ref().map(|c| c.capabilities()),
                calendars.as_ref().map(|c| c.capabilities()),
                submission.is_some(),
            );
            let registry = Arc::new(FolderRegistry::from_lists(folders, shared));
            let data_cap = cfg.pool_cap.saturating_sub(1).max(1);
            let pool = Arc::new(Pool::new(
                Arc::clone(&cfg),
                conn,
                data_cap,
                meter,
                Arc::clone(&bandwidth_cap),
            ));
            let account = ImapAccount::new(ImapAccountParts {
                config: cfg,
                capabilities: caps,
                pool,
                folders: registry,
                qresync_enabled: qresync.enabled,
                qresync_negotiation_warning: qresync.warning,
                bandwidth_cap,
                contacts,
                calendars,
                submission,
                dav_degraded,
            });
            Ok(Arc::new(account) as Arc<dyn Account>)
        })
    }
}

/// Outcome of attempting to attach a composed DAV sub-account. Never an
/// `Err`: a configured-but-failed open degrades to IMAP-only for this
/// open cycle rather than failing the whole IMAP account (brick 5).
enum DavAttach {
    /// Opened and attached.
    Attached(Arc<dyn Account>),
    /// Not configured.
    None,
    /// Configured but open failed; attach IMAP-only this cycle.
    Degraded {
        warning: bifrost_types::Warning,
        // Retried on the engine's next reopen by design. Carried for
        // telemetry/clarity; the engine's reopen re-runs the open either
        // way.
        #[allow(dead_code)]
        transient: bool,
    },
}

impl DavAttach {
    /// Resolve to the optional sub-account handle, pushing any degraded
    /// warning into `degraded` so the first discovery surfaces it.
    fn into_attached(self, degraded: &mut Vec<bifrost_types::Warning>) -> Option<Arc<dyn Account>> {
        match self {
            DavAttach::Attached(account) => Some(account),
            DavAttach::None => None,
            DavAttach::Degraded { warning, .. } => {
                degraded.push(warning);
                None
            }
        }
    }
}

/// Classify a DAV `open` outcome into an attach decision. A success
/// attaches; a failure degrades, routing through the central
/// `RecoveryClass` (read `reference/error-model.md`) to decide whether
/// the degradation is transient (retried on the engine's next reopen) or
/// a terminal config-fix.
fn classify_dav_open(result: Result<Arc<dyn Account>, AccountError>, label: &str) -> DavAttach {
    match result {
        Ok(account) => DavAttach::Attached(account),
        Err(error) => {
            // A retryable recovery class means the failure is transient
            // (transport blip, server unavailable, throttle); anything
            // terminal (auth lost, no permission, not found) needs an
            // operator config fix. Either way IMAP stays online.
            let transient = error.recovery().is_retryable();
            let kind = if transient {
                bifrost_types::WarningKind::Other
            } else {
                bifrost_types::WarningKind::OperatorAttentionNeeded
            };
            let warning = bifrost_types::Warning::support_only(
                kind,
                format!(
                    "{label} sub-account did not open ({}); continuing IMAP-only{}",
                    error.message_key(),
                    if transient {
                        ", will retry on reopen"
                    } else {
                        "; check DAV configuration"
                    }
                ),
            )
            .with_protocol_detail(bifrost_types::DiagnosticText::support_only(
                label.to_string(),
            ));
            DavAttach::Degraded { warning, transient }
        }
    }
}

async fn open_carddav(cfg: &ImapAccountConfig, account_id: AccountId) -> DavAttach {
    let Some(config) = cfg.carddav.clone() else {
        return DavAttach::None;
    };
    classify_dav_open(
        CardDavAccountFactory::new(config).open(account_id).await,
        "CardDAV",
    )
}

fn open_submission(
    cfg: &ImapAccountConfig,
) -> Result<Option<Arc<SubmissionTransport>>, AccountError> {
    let Some(config) = &cfg.submission else {
        return Ok(None);
    };
    let transport = SubmissionTransport::build(config, &cfg.credentials).map_err(discover_err)?;
    Ok(Some(Arc::new(transport)))
}

async fn open_caldav(cfg: &ImapAccountConfig, account_id: AccountId) -> DavAttach {
    let Some(config) = cfg.caldav.clone() else {
        return DavAttach::None;
    };
    classify_dav_open(
        CalDavAccountFactory::new(config).open(account_id).await,
        "CalDAV",
    )
}

fn meter_handle(
    cfg: &ImapAccountConfig,
    account_id: AccountId,
) -> Option<bifrost_net::MeterSinkHandle> {
    if let Some(meter) = &cfg.bandwidth_meter {
        return Some(bifrost_net::MeterSinkHandle::from_meter(
            Arc::clone(meter),
            account_id,
        ));
    }
    cfg.meter_sink
        .as_ref()
        .map(|sink| bifrost_net::MeterSinkHandle::new(Arc::clone(sink), account_id))
}

struct QresyncNegotiation {
    enabled: bool,
    warning: Option<String>,
}

async fn negotiate_qresync(
    conn: &crate::ImapConnection,
    cfg: &ImapAccountConfig,
    profile: &ServerProfile,
    server_id: &[(String, Option<String>)],
) -> Result<QresyncNegotiation, crate::Error> {
    if !cfg.enable_qresync || !profile.supports_qresync() {
        return Ok(QresyncNegotiation {
            enabled: false,
            warning: None,
        });
    }
    if server_id_disables_qresync(server_id) {
        return Ok(QresyncNegotiation {
            enabled: false,
            warning: Some(
                "server ID matches a known broken QRESYNC implementation; continuing with CONDSTORE"
                    .to_owned(),
            ),
        });
    }
    let enabled = conn.enable(&["QRESYNC"], cfg.imap.command_timeout).await?;
    let confirmed = enabled
        .iter()
        .any(|item| item.eq_ignore_ascii_case("QRESYNC"))
        || conn.server_profile().enabled("QRESYNC");
    Ok(QresyncNegotiation {
        enabled: confirmed,
        warning: (!confirmed).then(|| {
            "server advertised QRESYNC but ENABLE did not confirm it; continuing with CONDSTORE"
                .to_owned()
        }),
    })
}

async fn read_server_id(
    conn: &crate::ImapConnection,
    cfg: &ImapAccountConfig,
    profile: &ServerProfile,
) -> Result<Vec<(String, Option<String>)>, crate::Error> {
    if !profile.supports(Capability::Id) {
        return Ok(Vec::new());
    }
    match conn
        .id(&[("name", Some("bifrost-imap"))], cfg.imap.command_timeout)
        .await
    {
        Ok(id) => Ok(id),
        Err(_) => Ok(Vec::new()),
    }
}

fn server_id_disables_qresync(server_id: &[(String, Option<String>)]) -> bool {
    server_id.iter().any(|(key, value)| {
        let Some(value) = value.as_deref() else {
            return false;
        };
        let name = value.trim();
        key.eq_ignore_ascii_case("name")
            && (name.eq_ignore_ascii_case("icloud") || name.eq_ignore_ascii_case("icloud imap"))
    })
}

/// One shared/other-user folder discovered under a non-personal
/// namespace, carrying its owning mailbox identity for membership tagging
/// and scoped recovery.
pub(crate) struct SharedFolder {
    pub(crate) info: MailboxInfo,
    /// The owning mailbox. For an other-user namespace (root `#user/`),
    /// this is the per-principal segment that follows the root in each
    /// folder's own path - `#user/alice/INBOX` -> `MailboxId("alice")` -
    /// so distinct users get distinct owners. For a shared namespace
    /// (root `#shared.`), all folders share one owner, the root minus its
    /// trailing delimiter -> `MailboxId("#shared")`.
    pub(crate) owner: bifrost_types::MailboxId,
}

/// Owner identity for the *shared* namespace: the namespace root minus its
/// trailing delimiter. All folders under a shared root collapse to this one
/// owner (there is no per-principal segment in a shared namespace). Pure and
/// tested.
pub(crate) fn mailbox_owner_for(prefix: &str, delimiter: Option<char>) -> bifrost_types::MailboxId {
    // The owner is the whole namespace root minus its trailing delimiter,
    // NOT just the root's final segment. Collapsing to the final segment
    // merges distinct multi-segment shared trees: `#shared/dept/` and
    // `#other/dept/` would both yield `dept`, conflating two unrelated
    // shared mailboxes' membership scopes. The full (trailing-stripped)
    // prefix keeps them distinct.
    let owner = match delimiter {
        Some(d) => prefix.strip_suffix(d).unwrap_or(prefix),
        None => prefix,
    };
    bifrost_types::MailboxId(owner.to_owned())
}

/// Owner identity for the *other-user* namespace: the per-principal segment
/// that follows the namespace root in a folder's own path. RFC 2342 returns
/// a single `other` descriptor for the whole root (`#user/`), not one per
/// user, so the owning principal can only be read off each folder path -
/// `#user/alice/INBOX` under root `#user/` (delimiter `/`) -> `alice`;
/// `#user/bob` -> `bob`. Distinct users therefore get distinct owners.
/// Falls back to the root-derived owner when the folder path does not sit
/// under the prefix or has no segment after it (defensive; a well-formed
/// LIST under the prefix always does). Pure and tested.
pub(crate) fn mailbox_owner_from_other_user_path(
    folder_path: &str,
    prefix: &str,
    delimiter: Option<char>,
) -> bifrost_types::MailboxId {
    // The prefix only attributes a principal when the folder genuinely
    // sits *under* it: a bare textual `strip_prefix` would mis-read
    // `OtherTeam/INBOX` as belonging to the `Other` namespace and invent
    // owner `Team`. Require the prefix to terminate on a delimiter
    // boundary (the prefix already ends in the delimiter, or the path's
    // next character after the prefix is the delimiter). When it does
    // not, fall back to the root-derived owner rather than fabricating
    // one from a partial-segment match.
    let relative = match (folder_path.strip_prefix(prefix), delimiter) {
        (Some(rest), Some(d)) if prefix.ends_with(d) || rest.starts_with(d) => rest,
        (Some(rest), None) => rest,
        // Prefix matched textually but not on a delimiter boundary
        // (e.g. prefix `Other`, path `OtherTeam/INBOX`): not under the
        // namespace.
        _ => return mailbox_owner_for(prefix, delimiter),
    };
    let segment = match delimiter {
        Some(d) => relative.split(d).find(|s| !s.is_empty()),
        None => (!relative.is_empty()).then_some(relative),
    };
    match segment {
        Some(owner) => bifrost_types::MailboxId(owner.to_owned()),
        None => mailbox_owner_for(prefix, delimiter),
    }
}

/// Issue NAMESPACE (when advertised / rev2) and LIST each non-empty
/// `other` and `shared` namespace prefix. Returns the discovered shared
/// folders tagged with their owning mailbox. NAMESPACE absent or empty
/// other/shared lists -> empty Vec (a plain personal-only server). A LIST
/// under one prefix failing is non-fatal: log + skip that prefix, keep the
/// others (a revoked prefix must not fail the whole open). When the server
/// advertises ACL, each candidate folder is gated by MYRIGHTS: folders the
/// user cannot read are dropped before registration (do not surface a
/// scope you cannot SELECT). When ACL is not advertised, LIST visibility is
/// taken to imply at least lookup, and a later SELECT surfaces any `NO`.
pub(crate) async fn discover_shared_folders(
    conn: &crate::ImapConnection,
    cfg: &ImapAccountConfig,
    profile: &ServerProfile,
) -> Vec<SharedFolder> {
    if !profile.supports(Capability::Namespace) && !profile.imap4rev2 {
        return Vec::new();
    }
    let namespaces = match conn.namespace(cfg.imap.command_timeout).await {
        Ok(ns) => ns,
        Err(err) => {
            tracing::debug!(error = %err, "NAMESPACE failed; treating as personal-only");
            return Vec::new();
        }
    };
    let acl = profile.supports(Capability::Acl);
    let mut out = Vec::new();
    // Other-user and shared namespaces are both non-personal, but their
    // owner derivation differs: an other-user root (`#user/`) carries one
    // descriptor for ALL users, so the owning principal is read per-folder
    // (the segment after the root). A shared root (`#shared.`) has no
    // per-principal segment, so every folder under it shares one owner.
    let descriptors = namespaces
        .other
        .iter()
        .map(|d| (d, true))
        .chain(namespaces.shared.iter().map(|d| (d, false)));
    for (descriptor, is_other_user) in descriptors {
        if descriptor.prefix.is_empty() {
            continue;
        }
        let shared_owner = mailbox_owner_for(&descriptor.prefix, descriptor.delimiter);
        // LIST everything under this prefix. `LIST "<prefix>" "*"` enumerates
        // the namespace's folders.
        let listed = match conn
            .list(&descriptor.prefix, "*", cfg.imap.command_timeout)
            .await
        {
            Ok(folders) => folders,
            Err(err) => {
                tracing::debug!(
                    prefix = %descriptor.prefix,
                    error = %err,
                    "LIST under shared namespace prefix failed; skipping prefix"
                );
                continue;
            }
        };
        for info in listed {
            let owner = if is_other_user {
                mailbox_owner_from_other_user_path(
                    info.name.as_str(),
                    &descriptor.prefix,
                    descriptor.delimiter,
                )
            } else {
                shared_owner.clone()
            };
            let selectable = !info.attributes.iter().any(|attr| {
                matches!(
                    attr,
                    crate::types::MailboxAttribute::NoSelect
                        | crate::types::MailboxAttribute::NonExistent
                )
            });
            if acl && selectable {
                // Pre-flight ACL gate: skip folders we cannot read. The
                // rights gate is advisory; a per-folder MYRIGHTS failure is
                // non-fatal (log + keep the folder; SELECT stays
                // authoritative).
                match conn
                    .my_rights(info.name.as_str(), cfg.imap.command_timeout)
                    .await
                {
                    Ok(rights) => {
                        if !MailboxRights::parse(&rights).can_read() {
                            continue;
                        }
                    }
                    Err(err) => {
                        tracing::debug!(
                            mailbox = %info.name.as_str(),
                            error = %err,
                            "MYRIGHTS failed for shared folder; registering and deferring to SELECT"
                        );
                    }
                }
            }
            out.push(SharedFolder { info, owner });
        }
    }
    out
}

pub(crate) async fn list_folders(
    conn: &crate::ImapConnection,
    cfg: &ImapAccountConfig,
    profile: &ServerProfile,
) -> Result<Vec<MailboxInfo>, crate::Error> {
    if profile.supports(Capability::ListExtended) || profile.imap4rev2 {
        match conn
            .list_extended("", &["*"], &[], &["SPECIAL-USE"], cfg.imap.command_timeout)
            .await
        {
            Ok(folders) => return Ok(folders),
            Err(crate::Error::MissingCapability(_)) => {}
            Err(err) => return Err(err),
        }
    }
    conn.list("", "*", cfg.imap.command_timeout).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mailbox_owner_for_strips_delimiter() {
        // Shared-root helper: trailing-delimiter-stripped final segment.
        // Single-segment shared prefix: no interior delimiter, whole
        // (trailing-stripped) prefix is the owner.
        assert_eq!(
            mailbox_owner_for("#shared.", Some('.')),
            bifrost_types::MailboxId("#shared".to_owned())
        );
        // No delimiter: prefix is the owner verbatim.
        assert_eq!(
            mailbox_owner_for("Shared", None),
            bifrost_types::MailboxId("Shared".to_owned())
        );
        // Multi-segment shared root keeps its whole (trailing-stripped)
        // path as the owner so distinct trees do not collapse. A prior
        // version used only the final segment, which merged
        // `#shared/dept/` and `#other/dept/` into one `dept` owner.
        assert_eq!(
            mailbox_owner_for("#shared/dept/", Some('/')),
            bifrost_types::MailboxId("#shared/dept".to_owned())
        );
        assert_ne!(
            mailbox_owner_for("#shared/dept/", Some('/')),
            mailbox_owner_for("#other/dept/", Some('/')),
        );
    }

    #[test]
    fn mailbox_owner_from_other_user_path_reads_principal_per_folder() {
        // RFC 2342 returns one `other` descriptor (`#user/`) for every
        // user. The owning principal must be read off each folder path so
        // distinct users get distinct owners - not collapsed to the root.
        assert_eq!(
            mailbox_owner_from_other_user_path("#user/alice/INBOX", "#user/", Some('/')),
            bifrost_types::MailboxId("alice".to_owned())
        );
        assert_eq!(
            mailbox_owner_from_other_user_path("#user/bob/Sent", "#user/", Some('/')),
            bifrost_types::MailboxId("bob".to_owned())
        );
        // The user-root folder itself (no segment after the principal).
        assert_eq!(
            mailbox_owner_from_other_user_path("#user/alice", "#user/", Some('/')),
            bifrost_types::MailboxId("alice".to_owned())
        );
        // Non-default delimiter.
        assert_eq!(
            mailbox_owner_from_other_user_path(
                "Other Users.carol.INBOX",
                "Other Users.",
                Some('.')
            ),
            bifrost_types::MailboxId("carol".to_owned())
        );
        // Defensive fallback: a path not under the prefix degrades to the
        // root-derived owner rather than panicking.
        assert_eq!(
            mailbox_owner_from_other_user_path("#user/", "#user/", Some('/')),
            bifrost_types::MailboxId("#user".to_owned())
        );
        // Delimiter-boundary guard: a prefix that matches textually but
        // not on a delimiter boundary (`Other` vs `OtherTeam/INBOX`) must
        // NOT invent a bogus principal (`Team`) from the partial-segment
        // match; it falls back to the root-derived owner.
        assert_eq!(
            mailbox_owner_from_other_user_path("OtherTeam/INBOX", "Other", Some('/')),
            bifrost_types::MailboxId("Other".to_owned())
        );
        // A prefix without a trailing delimiter that DOES terminate on a
        // boundary (next char is the delimiter) still reads the principal.
        assert_eq!(
            mailbox_owner_from_other_user_path("Other/dave/INBOX", "Other", Some('/')),
            bifrost_types::MailboxId("dave".to_owned())
        );
    }

    #[test]
    fn qresync_runtime_gate_defaults_off() {
        let cfg = ImapAccountConfig::new(
            ImapConfig::tls("imap.example.test"),
            Credentials::password("user", "pass"),
            AuthPolicy::default(),
        );
        assert!(!cfg.enable_qresync);
    }

    #[test]
    fn server_id_preconfigures_known_broken_qresync() {
        let id = vec![("name".to_string(), Some("iCloud IMAP".to_string()))];
        assert!(server_id_disables_qresync(&id));

        let id = vec![("name".to_string(), Some("Dovecot".to_string()))];
        assert!(!server_id_disables_qresync(&id));

        let id = vec![("name".to_string(), Some("MyIcloudProxy".to_string()))];
        assert!(!server_id_disables_qresync(&id));
    }

    fn transport_error() -> AccountError {
        bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network),
            bifrost_types::Cause::Transport(bifrost_types::TransportCause::new(
                bifrost_types::TransportKind::Network,
                None,
            )),
        )
        .operation(AccountOperation::Discover)
        .try_build()
        .expect("valid account error classification")
    }

    fn auth_lost_error() -> AccountError {
        bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Authentication(
                bifrost_types::AuthErrorKind::ReauthorizationRequired,
            ),
            bifrost_types::Cause::Auth(bifrost_types::AuthCause::ReauthorizationRequired),
        )
        .operation(AccountOperation::Discover)
        .try_build()
        .expect("valid account error classification")
    }

    #[test]
    fn dav_open_classify_maps_transient_and_terminal_and_success() {
        // Transport failure -> degraded, transient.
        match classify_dav_open(Err(transport_error()), "CardDAV") {
            DavAttach::Degraded { transient, .. } => assert!(transient, "transport is transient"),
            other => panic!("expected Degraded, got {}", attach_label(&other)),
        }

        // Auth-lost -> degraded, terminal (not transient).
        match classify_dav_open(Err(auth_lost_error()), "CardDAV") {
            DavAttach::Degraded { transient, .. } => {
                assert!(!transient, "auth-lost is a terminal config fix");
            }
            other => panic!("expected Degraded, got {}", attach_label(&other)),
        }

        // Success -> attached.
        let stub = crate::account::test_support::stub_arc(
            crate::account::test_support::StubAccount::new(Vec::new()),
        );
        match classify_dav_open(Ok(stub), "CardDAV") {
            DavAttach::Attached(_) => {}
            other => panic!("expected Attached, got {}", attach_label(&other)),
        }
    }

    fn attach_label(attach: &DavAttach) -> &'static str {
        match attach {
            DavAttach::Attached(_) => "Attached",
            DavAttach::None => "None",
            DavAttach::Degraded { .. } => "Degraded",
        }
    }
}
