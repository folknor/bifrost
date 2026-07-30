use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bifrost_net::{StaticTokenSource, TokenSource};
use bifrost_types::{
    Account, AccountError, AccountFactory, AccountFuture, AccountId, CursorScope, ObjectType,
    OpenedAccount, SkippedScope,
};

use tokio_util::sync::CancellationToken;

use crate::account::Account as JmapMailAccount;
use crate::client::{Client, Credentials};
use crate::core::capability;
use crate::core::capability::Capability;
use crate::core::id::AccountId as JmapAccountId;
use crate::core::transport::HttpTransport;
use crate::email::{EmailGet, EmailId};
use crate::mailbox::{MailboxGet, Property as MailboxProperty};
use crate::principal::PrincipalGet;
use crate::transport_reqwest::ReqwestTransport;

use super::account::JmapAccount;
use super::capabilities;
use super::foreign;
use super::push::{PushRouting, ReconnectPolicy, WsState};
use super::state;

type MailAccount = JmapMailAccount<ReqwestTransport>;

// pub: re-exported through crate::sync for engine AccountFactory registration.
#[derive(Debug, Clone)]
pub struct JmapAccountFactory {
    config: JmapAccountFactoryBuilder,
}

// pub: returned by JmapAccountFactory::builder for open-time configuration.
#[derive(Debug, Clone)]
pub struct JmapAccountFactoryBuilder {
    url: String,
    credentials: JmapCredentials,
    timeout: Option<Duration>,
    accept_invalid_certs: bool,
    reconnect_policy: ReconnectPolicy,
}

// pub: caller-supplied auth material for constructing a JMAP AccountFactory.
#[derive(Clone)]
#[non_exhaustive]
pub enum JmapCredentials {
    Basic { username: String, password: String },
    // The bearer source is shared: ratatoskr hands in one
    // `Arc<dyn TokenSource>` (typically an `OAuthRefresher` over its own
    // refresh-token store) and a token it refreshes and persists is read
    // live at every wire authentication without reopening the account.
    Bearer { token_source: Arc<dyn TokenSource> },
}

// `Arc<dyn TokenSource>` is not `Debug`; redact the bearer source so the
// derived `Debug` on the surrounding factory structs keeps working
// without leaking token material.
impl std::fmt::Debug for JmapCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::Bearer { .. } => f
                .debug_struct("Bearer")
                .field("token_source", &"<token-source>")
                .finish(),
        }
    }
}

impl JmapAccountFactory {
    pub fn new(url: impl Into<String>, credentials: JmapCredentials) -> Self {
        Self {
            config: JmapAccountFactoryBuilder::new(url, credentials),
        }
    }

    pub fn builder(
        url: impl Into<String>,
        credentials: JmapCredentials,
    ) -> JmapAccountFactoryBuilder {
        JmapAccountFactoryBuilder::new(url, credentials)
    }
}

impl JmapAccountFactoryBuilder {
    pub fn new(url: impl Into<String>, credentials: JmapCredentials) -> Self {
        Self {
            url: url.into(),
            credentials,
            timeout: None,
            accept_invalid_certs: false,
            reconnect_policy: ReconnectPolicy::default(),
        }
    }

    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    #[must_use]
    pub fn accept_invalid_certs(mut self, accept_invalid_certs: bool) -> Self {
        self.accept_invalid_certs = accept_invalid_certs;
        self
    }

    #[must_use]
    pub fn reconnect_policy(mut self, reconnect_policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = reconnect_policy;
        self
    }

    #[must_use]
    pub fn build(self) -> JmapAccountFactory {
        JmapAccountFactory { config: self }
    }
}

impl AccountFactory for JmapAccountFactory {
    fn open(&self, account_id: AccountId) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let config = self.config.clone();
        Box::pin(async move {
            let client = connect(config.clone(), account_id).await.map_err(|err| {
                super::error::into_account_error(
                    err,
                    super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Discover)
                        .with_scope(bifrost_types::ErrorScope::Account),
                )
            })?;
            let mail = client
                .primary_account::<capability::Mail>()
                .map_err(|err| {
                    super::error::into_account_error(
                        err,
                        super::error::JmapErrorContext::new(
                            bifrost_types::AccountOperation::Discover,
                        )
                        .with_scope(bifrost_types::ErrorScope::Account),
                    )
                })?;
            let submission = client.primary_account::<capability::Submission>().ok();
            let vacation = client
                .primary_account::<capability::VacationResponseCap>()
                .ok();
            let quota = client.primary_account::<capability::Quota>().ok();
            let sieve = client.primary_account::<capability::Sieve>().ok();
            let contacts = client.primary_account::<capability::Contacts>().ok();
            let calendars = client.primary_account::<capability::Calendars>().ok();
            let session = client.session();
            let self_emails = fetch_self_emails(&client, &config.credentials).await;
            let max_delayed_send = match session.submission_capabilities() {
                Some(caps) => caps.max_delayed_send(),
                None => 0,
            };
            let (email_state, mailbox_state, mailbox_names) =
                seed_account_state(&mail).await.map_err(|err| {
                    super::error::into_account_error(
                        err,
                        super::error::JmapErrorContext::new(
                            bifrost_types::AccountOperation::Discover,
                        )
                        .with_scope(bifrost_types::ErrorScope::Account),
                    )
                })?;

            let mut seed_states = HashMap::new();
            seed_states.insert(
                CursorScope::Type(ObjectType::Email),
                state::encode_for_scope(&CursorScope::Type(ObjectType::Email), email_state.clone())
                    .expect("static email cursor scope is supported"),
            );
            seed_states.insert(
                CursorScope::Type(ObjectType::Mailbox),
                state::encode_for_scope(
                    &CursorScope::Type(ObjectType::Mailbox),
                    mailbox_state.clone(),
                )
                .expect("static mailbox cursor scope is supported"),
            );

            // Per-accountId state caches. The primary account's id keys
            // the same maps every foreign account does - no primary-vs-
            // foreign branch in the cache itself.
            let primary_id = mail.id_str().to_string();
            let mut email_states: HashMap<String, Option<String>> = HashMap::new();
            let mut mailbox_states: HashMap<String, Option<String>> = HashMap::new();
            email_states.insert(primary_id.clone(), Some(email_state.clone()));
            mailbox_states.insert(primary_id.clone(), Some(mailbox_state.clone()));

            // Foreign (shared/delegate) accounts: the session lists every
            // non-personal mail account. For each, probe its two states
            // and seed one `Folder` cursor scope per mailbox. A probe
            // failure - revoked grant and exhausted transient retry
            // alike - skips that foreign account and records the
            // omission on `OpenedAccount::skipped_scopes` with its
            // classified error. Open itself must not fail here: initial
            // attach does not retry `factory.open`, so failing would
            // block the user's own primary mail on someone else's
            // shared mailbox being down. And skipping silently would
            // erase the share for the session with no signal anywhere.
            let foreign_ids = foreign_mail_account_ids(&session, &primary_id);
            let mut foreign_mail: HashMap<String, MailAccount> = HashMap::new();
            let mut foreign_submission = HashSet::new();
            let mut skipped_scopes: Vec<SkippedScope> = Vec::new();
            for foreign_id in foreign_ids {
                let foreign_account =
                    MailAccount::new(client.clone(), JmapAccountId::new(&foreign_id));
                match seed_foreign_account_or_skip(&foreign_id, &foreign_account).await {
                    Ok(seed) => {
                        email_states.insert(foreign_id.clone(), Some(seed.email_state));
                        mailbox_states.insert(foreign_id.clone(), Some(seed.mailbox_state));
                        for mailbox_id in seed.mailbox_ids {
                            let scope = CursorScope::Folder(foreign::encode_foreign(
                                &foreign_id,
                                &mailbox_id,
                            ));
                            // Foreign email changes track the account's
                            // Email state; seed each mailbox's Folder
                            // cursor from it.
                            if let Ok(encoded) =
                                state::encode_for_scope(&scope, seed.email_state_for_seed.clone())
                            {
                                seed_states.insert(scope, encoded);
                            }
                        }
                        if account_advertises_submission(&session, &foreign_id) {
                            foreign_submission.insert(foreign_id.clone());
                        }
                        foreign_mail.insert(foreign_id, foreign_account);
                    }
                    Err(skip) => {
                        // Leave the share out of this session's surface
                        // and report the omission. Reopen re-reads the
                        // session, so a restored grant or a recovered
                        // server brings the share back.
                        skipped_scopes.push(skip);
                    }
                }
            }

            // This gate is derived from the successfully seeded routing set,
            // never merely from the session. A foreign account skipped during
            // probing must not make us advertise an unreachable send-as path.
            let support = capabilities::PimSupport {
                submission: submission.is_some(),
                max_delayed_send,
                foreign_submission: !foreign_submission.is_empty(),
                vacation: vacation.is_some(),
                quota: quota.is_some(),
                sieve: sieve.is_some(),
                contacts: contacts.is_some(),
                calendar: calendars.is_some(),
            };
            let (caps, limits) = capabilities::build(&session, support)?;

            let shutdown = CancellationToken::new();
            // Snapshot of who this session syncs: push notifications are
            // keyed by accountId (RFC 8620 s7.1) and the reader routes a
            // foreign account's Email changes onto its seeded `Folder`
            // scopes instead of the primary type scope.
            let push_routing = Arc::new(PushRouting::new(primary_id.clone(), seed_states.keys()));
            let ws = WsState::spawn(
                client.clone(),
                caps.push_in_process(),
                shutdown.clone(),
                config.reconnect_policy,
                push_routing,
            );

            let account = JmapAccount::new(
                client,
                mail,
                foreign_mail,
                foreign_submission,
                submission,
                max_delayed_send,
                vacation,
                quota,
                sieve,
                contacts,
                calendars,
                self_emails,
                caps,
                limits,
                seed_states,
                ws,
                shutdown,
                email_states,
                mailbox_states,
                mailbox_names,
            );

            Ok(OpenedAccount {
                account: Arc::new(account) as Arc<dyn Account>,
                skipped_scopes,
            })
        })
    }
}

fn account_advertises_submission(session: &crate::core::session::Session, id: &str) -> bool {
    session.account(id).is_some_and(|account| {
        account
            .capabilities()
            .any(|uri| uri.as_str() == <capability::Submission as Capability>::URI)
    })
}

async fn fetch_self_emails(client: &Client, credentials: &JmapCredentials) -> Vec<String> {
    let mut emails = Vec::new();
    if let Some(email) = credentials.configured_email() {
        push_email_alias(&mut emails, email);
    }

    let Some(principal_id) = client
        .session()
        .principals_capabilities()
        .and_then(|capabilities| capabilities.current_user_principal_id().cloned())
        .or_else(|| {
            client
                .session()
                .principals_owner_capabilities()
                .and_then(|capabilities| capabilities.principal_id().cloned())
        })
    else {
        return emails;
    };

    let Some(principals) = principal_account(client) else {
        return emails;
    };
    let Ok(response) = principals
        .call(PrincipalGet::new().ids([principal_id]))
        .await
    else {
        return emails;
    };
    for principal in response.into_list() {
        if let Some(email) = principal.email() {
            push_email_alias(&mut emails, email);
        }
        if let Some(aliases) = principal.aliases() {
            for alias in aliases {
                push_email_alias(&mut emails, alias);
            }
        }
    }
    emails
}

fn principal_account(
    client: &Client,
) -> Option<crate::account::Account<crate::transport_reqwest::ReqwestTransport>> {
    client
        .session()
        .principals_owner_capabilities()
        .and_then(|capabilities| capabilities.account_id_for_principal().cloned())
        .map(|account_id| crate::account::Account::new(client.clone(), account_id))
        .or_else(|| client.primary_account::<capability::Principals>().ok())
}

fn push_email_alias(emails: &mut Vec<String>, email: &str) {
    if !email.contains('@') {
        return;
    }
    let normalized = email.to_ascii_lowercase();
    if !emails.iter().any(|existing| existing == &normalized) {
        emails.push(normalized);
    }
}

/// The session's non-personal mail accounts (foreign / shared /
/// delegate), excluding the primary mail account. JMAP servers list
/// shared/delegated accounts with `isPersonal: false` and their own
/// `accountCapabilities`; A5a auto-discovers them from the session (no
/// config needed, unlike Graph). Reading the session document alone -
/// and every `open` fetches a fresh one - is what backs the advertised
/// `reopen_discovers_foreign_namespaces` capability: a share granted
/// after the last open appears here on the next open.
fn foreign_mail_account_ids(
    session: &crate::core::session::Session,
    primary_id: &str,
) -> Vec<String> {
    let mut ids: Vec<String> = session
        .accounts()
        .filter(|id| id.as_str() != primary_id)
        .filter(|id| {
            session.account(id).is_some_and(|account| {
                !account.is_personal()
                    && account
                        .capabilities()
                        .any(|uri| uri.as_str() == <capability::Mail as Capability>::URI)
            })
        })
        .cloned()
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

struct ForeignSeed {
    email_state: String,
    mailbox_state: String,
    /// The foreign account's mailbox ids, one `Folder` cursor scope per.
    mailbox_ids: Vec<String>,
    /// The Email state seeded into each foreign `Folder` cursor (the
    /// foreign account's `Email/changes` state).
    email_state_for_seed: String,
}

/// Probe one foreign account's `Email` / `Mailbox` states and enumerate its
/// mailboxes. Mirrors the primary probes.
async fn seed_foreign_account<T: HttpTransport>(
    mail: &JmapMailAccount<T>,
) -> crate::Result<ForeignSeed> {
    let (email_state, mailbox_state, mailbox_names) = seed_account_state(mail).await?;
    let mailbox_ids: Vec<String> = mailbox_names.into_keys().collect();
    Ok(ForeignSeed {
        email_state_for_seed: email_state.clone(),
        email_state,
        mailbox_state,
        mailbox_ids,
    })
}

/// Seed one foreign account, or turn its probe failure into the
/// open-time skip entry the factory records on
/// `OpenedAccount::skipped_scopes`.
///
/// Every failure lane lands here: a revoked grant classifies terminal
/// (`NoPermission`) and an exhausted transient retry classifies
/// retryable. Neither is allowed to take either unacceptable shape: a
/// probe failure must not fail the whole open (initial attach does not
/// retry `factory.open`, so that blocks the user's own primary mail on
/// a delegate outage), and it must not skip silently (that erases the
/// share for the session, the original G8 defect). The skip's scope
/// names the foreign accountId so the consumer knows WHICH share is
/// degraded and the recovery class says whether a reopen can heal it.
async fn seed_foreign_account_or_skip<T: HttpTransport>(
    foreign_id: &str,
    mail: &JmapMailAccount<T>,
) -> Result<ForeignSeed, SkippedScope> {
    match seed_foreign_account(mail).await {
        Ok(seed) => Ok(seed),
        Err(err) => {
            let error = super::error::into_account_error(
                err,
                super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Discover)
                    .with_scope(bifrost_types::ErrorScope::Mailbox {
                        id: foreign_id.to_string(),
                    }),
            );
            Err(SkippedScope {
                scope: bifrost_types::ErrorScope::Mailbox {
                    id: foreign_id.to_string(),
                },
                error,
            })
        }
    }
}

/// The open-time probe set: `Email/get`, `Mailbox/get`.
const OPEN_PROBE_CALLS: usize = 2;

type OpenProbeResponses = (
    crate::core::get::GetResponse<crate::email::Email>,
    crate::core::get::GetResponse<crate::mailbox::Mailbox>,
);

/// Fresh copies of the two open-time probes, in wire order.
fn open_probe_methods() -> (EmailGet, MailboxGet) {
    (
        EmailGet::new().ids(Vec::<EmailId>::new()),
        MailboxGet::new().properties([MailboxProperty::Id, MailboxProperty::Name]),
    )
}

/// Issue the two open-time probes, batched into one request when the
/// session says one request can hold them and serially when it does not.
///
/// `maxCallsInRequest` and `maxSizeRequest` are both hard limits
/// (RFC 8620 §2): exceeding either makes the server reject the *entire*
/// request with a request-level `limit` error, not just the surplus
/// calls. The commonly quoted 16 is the minimum a server is recommended
/// to support, never a floor a client may assume, so batching without
/// checking would fail a primary account's open outright and silently
/// drop every foreign account against a conservative server.
async fn open_probes<T: HttpTransport>(
    mail: &JmapMailAccount<T>,
) -> crate::Result<OpenProbeResponses> {
    if let Some(responses) = batched_open_probes(mail).await? {
        return Ok(responses);
    }
    let (email, mailboxes) = open_probe_methods();
    Ok((mail.call(email).await?, mail.call(mailboxes).await?))
}

/// The batched leg. Returns `Ok(None)` without sending anything when
/// the advertised limits cannot hold both calls in one request.
async fn batched_open_probes<T: HttpTransport>(
    mail: &JmapMailAccount<T>,
) -> crate::Result<Option<OpenProbeResponses>> {
    let session = mail.client().session();
    // A session with no advertised core capability tells us nothing
    // about its limits, and `capabilities::build` rejects it later in
    // open anyway. Take the conservative shape rather than guessing.
    let Some(core) = session.core_capabilities() else {
        return Ok(None);
    };
    if core.max_calls_in_request() < OPEN_PROBE_CALLS {
        return Ok(None);
    }
    // The size limit is checked against the actual encoding inside
    // `send_methods_within`, because it can fall between the individual
    // probes and their batch.
    mail.build()
        .send_methods_within(open_probe_methods(), core.max_size_request())
        .await
}

/// Read the two states needed at open. Mailbox names ride the
/// Mailbox/get already needed for its state, so both the primary and
/// shared-account paths use exactly the same request shape.
async fn seed_account_state<T: HttpTransport>(
    mail: &JmapMailAccount<T>,
) -> crate::Result<(String, String, HashMap<String, String>)> {
    let (email, mailboxes) = open_probes(mail).await?;

    let email_state = email.into_state();
    let mailbox_state = mailboxes.state().to_string();
    let mut mailbox_names = HashMap::new();
    for mut mailbox in mailboxes.into_list() {
        let id = mailbox.take_id();
        if !id.as_str().is_empty() {
            mailbox_names.insert(id.into_string(), mailbox.name().unwrap_or("").to_string());
        }
    }

    Ok((email_state, mailbox_state, mailbox_names))
}

async fn connect(
    config: JmapAccountFactoryBuilder,
    account_id: AccountId,
) -> crate::Result<Client> {
    let mut builder = Client::new()
        .credentials(config.credentials.into_client_credentials())
        .net_account_id(account_id)
        .accept_invalid_certs(config.accept_invalid_certs);
    if let Some(timeout) = config.timeout {
        builder = builder.timeout(timeout);
    }
    builder.connect(&config.url).await
}

impl JmapCredentials {
    #[must_use]
    pub fn bearer(token: impl Into<String>) -> Self {
        Self::Bearer {
            token_source: Arc::new(StaticTokenSource::new(token, None)),
        }
    }

    /// Construct bearer credentials from a shared token source.
    /// ratatoskr supplies one `Arc<dyn TokenSource>` it also drives
    /// rotation on, so a rotated token is visible to JMAP through this
    /// single object with no reopen.
    #[must_use]
    pub fn bearer_source(source: Arc<dyn TokenSource>) -> Self {
        Self::Bearer {
            token_source: source,
        }
    }

    fn configured_email(&self) -> Option<&str> {
        match self {
            Self::Basic { username, .. } if username.contains('@') => Some(username),
            _ => None,
        }
    }

    fn into_client_credentials(self) -> Credentials {
        match self {
            Self::Basic { username, password } => Credentials::basic(&username, &password),
            Self::Bearer { token_source } => Credentials::bearer_source(token_source),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;
    use bytes::Bytes;
    use serde_json::{Value, json};

    use crate::core::session::Session;
    use crate::core::transport::TransportError;

    /// Scripted JMAP HTTP boundary with the same contract as
    /// `ReqwestTransport`.
    ///
    /// Fidelity is the whole point: a fixture states a status and a body,
    /// and the seam derives the error shape from the same decision table
    /// `crates/net/src/request.rs` walks, so a test cannot pin behaviour
    /// against a response shape the production stack is incapable of
    /// producing. Once armed, the script is closed: an unplanned request
    /// reports its ordinal instead of ever reaching a real HTTP transport.
    struct ScriptedTransport {
        requests: Mutex<Vec<Value>>,
        replies: Mutex<VecDeque<ScriptedReply>>,
    }

    enum ScriptedReply {
        /// A server response, classified on delivery.
        Response {
            status: reqwest::StatusCode,
            body: Bytes,
            headers: reqwest::header::HeaderMap,
        },
        /// A transport failure with no HTTP response behind it.
        Error(TransportError),
    }

    impl ScriptedReply {
        fn ok(body: impl Into<Bytes>) -> Self {
            Self::status(reqwest::StatusCode::OK, body)
        }

        fn status(status: reqwest::StatusCode, body: impl Into<Bytes>) -> Self {
            Self::Response {
                status,
                body: body.into(),
                headers: reqwest::header::HeaderMap::new(),
            }
        }

        fn header(mut self, name: &'static str, value: &str) -> Self {
            if let Self::Response { headers, .. } = &mut self {
                headers.insert(
                    name,
                    reqwest::header::HeaderValue::from_str(value).expect("test header value"),
                );
            }
            self
        }

        /// Reproduce what the production stack hands the JMAP layer for
        /// this response. Follows `crates/net/src/request.rs`:
        ///
        /// - 2xx: the body reaches the protocol decoder.
        /// - 3xx: bifrost-net returns 304 / 305 / 306 and `Location`-less
        ///   redirects as `Ok` with the body replaced by an empty stream
        ///   (the redirect loop's `PassThrough` arm discards passthrough
        ///   bodies), so `ReqwestTransport::handle_response` turns them
        ///   into a body-less `TransportError` with no net evidence
        ///   attached. The fixture drops any scripted body for the same
        ///   reason: production cannot deliver one.
        /// - 401: the forced refresh retries once, and the second 401 is
        ///   `AuthLost` with acknowledged transmission evidence.
        /// - Retryable per `RetryPolicy::default()` - 429 plus the whole
        ///   5xx family, read off the policy rather than restated: the
        ///   retry budget is spent and the loop surfaces `RateLimited`
        ///   for 429 and `RetryBudgetExhausted` for the rest, each with
        ///   the final response preserved.
        /// - Any other 4xx: terminal `Error::Status`.
        ///
        /// So a `Status` fixture for a retryable code is not something a
        /// test author can write by accident.
        fn into_transport_result(self) -> Result<Bytes, TransportError> {
            let (status, body, headers) = match self {
                Self::Error(error) => return Err(error),
                Self::Response {
                    status,
                    body,
                    headers,
                } => (status, body, headers),
            };
            if status.is_success() {
                return Ok(body);
            }
            if status.is_redirection() {
                return Err(TransportError::with_body(
                    format!("HTTP {status}"),
                    Bytes::new(),
                ));
            }
            let final_response = bifrost_net::FinalResponse {
                status,
                headers: headers.clone(),
                body: body.clone(),
            };
            if status == reqwest::StatusCode::UNAUTHORIZED {
                return Err(TransportError::from_net(bifrost_net::Error::AuthLost {
                    transmission_state: Some(bifrost_types::TransmissionState::Acknowledged),
                    final_response: Some(final_response),
                }));
            }
            let policy = bifrost_net::RetryPolicy::default();
            if policy.statuses.contains(&status) || status.is_server_error() {
                let retry_after =
                    bifrost_net::parse_retry_after(headers.get(reqwest::header::RETRY_AFTER))
                        .map(|hint| hint.min(policy.honor_retry_after_cap));
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    return Err(TransportError::from_net(bifrost_net::Error::RateLimited {
                        retry_after,
                        final_response,
                    }));
                }
                return Err(TransportError::from_net(
                    bifrost_net::Error::RetryBudgetExhausted {
                        final_response: Some(final_response),
                        retry_after_history: retry_after.into_iter().collect(),
                    },
                ));
            }
            Err(TransportError::from_net(bifrost_net::Error::Status {
                code: status,
                body,
                headers,
            }))
        }
    }

    impl ScriptedTransport {
        fn new(replies: impl IntoIterator<Item = ScriptedReply>) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                replies: Mutex::new(replies.into_iter().collect()),
            }
        }

        fn requests(&self) -> Vec<Value> {
            self.requests.lock().expect("script requests lock").clone()
        }

        fn api_reply(&self, body: Vec<u8>) -> Result<Bytes, TransportError> {
            let request = serde_json::from_slice(&body).map_err(|error| {
                TransportError::with_source("JMAP client emitted a non-JSON API request", error)
            })?;
            let request_number = {
                let mut requests = self
                    .requests
                    .lock()
                    .map_err(|_| TransportError::new("script requests lock poisoned"))?;
                requests.push(request);
                requests.len()
            };
            let reply = self
                .replies
                .lock()
                .map_err(|_| TransportError::new("script replies lock poisoned"))?
                .pop_front()
                .ok_or_else(|| {
                    TransportError::new(format!(
                        "scripted JMAP transport exhausted at API request #{request_number}"
                    ))
                })?;
            reply.into_transport_result()
        }
    }

    impl HttpTransport for ScriptedTransport {
        async fn api_request(&self, _url: &str, body: Vec<u8>) -> Result<Bytes, TransportError> {
            self.api_reply(body)
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            Err(TransportError::new(
                "scripted JMAP transport has no upload reply",
            ))
        }

        async fn download(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new(
                "scripted JMAP transport has no download reply",
            ))
        }

        async fn get_session(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new(
                "scripted JMAP transport has no session reply",
            ))
        }
    }

    fn session() -> Session {
        session_with_limits(8, 100_000)
    }

    fn session_with_limits(max_calls_in_request: usize, max_size_request: usize) -> Session {
        session_doc(
            max_calls_in_request,
            max_size_request,
            json!({
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}},
                "shared": {"name": "Shared", "isPersonal": false, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}
            }),
        )
    }

    fn session_doc(
        max_calls_in_request: usize,
        max_size_request: usize,
        accounts: Value,
    ) -> Session {
        serde_json::from_value(json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": max_size_request,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": max_calls_in_request,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": accounts,
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("test session parses")
    }

    fn email_result(account_id: &str, suffix: &str, call_id: &str) -> Value {
        json!(["Email/get", {"accountId": account_id, "state": format!("email-{suffix}"), "list": [], "notFound": []}, call_id])
    }

    fn mailbox_result(account_id: &str, suffix: &str, call_id: &str) -> Value {
        json!(["Mailbox/get", {"accountId": account_id, "state": format!("mailbox-{suffix}"), "list": [{"id": format!("inbox-{suffix}"), "name": "Inbox"}], "notFound": []}, call_id])
    }

    fn method_reply(results: Vec<Value>) -> ScriptedReply {
        ScriptedReply::ok(
            json!({"sessionState": "session-1", "methodResponses": results}).to_string(),
        )
    }

    fn method_error(account_id: &str, error_type: &str) -> ScriptedReply {
        method_reply(vec![json!([
            "error",
            {"type": error_type, "accountId": account_id},
            "s0"
        ])])
    }

    /// One reply answering both batched open probes.
    fn open_reply(account_id: &str, suffix: &str) -> ScriptedReply {
        method_reply(vec![
            email_result(account_id, suffix, "s0"),
            mailbox_result(account_id, suffix, "s1"),
        ])
    }

    /// Two replies, one per probe, for the serial fallback. Each
    /// single-call request numbers its one call `s0`.
    fn serial_open_replies(account_id: &str, suffix: &str) -> [ScriptedReply; 2] {
        [
            method_reply(vec![email_result(account_id, suffix, "s0")]),
            method_reply(vec![mailbox_result(account_id, suffix, "s0")]),
        ]
    }

    fn scripted_client(
        replies: impl IntoIterator<Item = ScriptedReply>,
    ) -> Client<ScriptedTransport> {
        scripted_client_with_session(session(), replies)
    }

    fn scripted_client_with_session(
        session: Session,
        replies: impl IntoIterator<Item = ScriptedReply>,
    ) -> Client<ScriptedTransport> {
        Client::with_transport(ScriptedTransport::new(replies), session)
            .expect("scripted client builds")
    }

    fn assert_open_batch(request: &Value, account_id: &str) {
        assert_eq!(request["methodCalls"].as_array().map(Vec::len), Some(2));
        assert_eq!(request["methodCalls"][0][0], "Email/get");
        assert_eq!(request["methodCalls"][1][0], "Mailbox/get");
        for call in request["methodCalls"]
            .as_array()
            .expect("method calls array")
        {
            assert_eq!(call[1]["accountId"], account_id);
        }
    }

    /// The `reopen_discovers_foreign_namespaces` capability promises
    /// that a share granted after the last open surfaces at the next
    /// one. The mechanism is that foreign discovery reads only the
    /// session document and every `open` fetches a fresh one (the
    /// factory carries no session cache), so the predicate must track a
    /// before/after pair of session documents - and must select only
    /// non-personal accounts that actually advertise mail.
    #[test]
    fn foreign_discovery_is_session_driven_so_a_new_grant_surfaces_at_reopen() {
        let personal_mail = json!({
            "name": "Primary",
            "isPersonal": true,
            "isReadOnly": false,
            "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}
        });

        let before = session_doc(8, 100_000, json!({"primary": personal_mail.clone()}));
        assert_eq!(
            foreign_mail_account_ids(&before, "primary"),
            Vec::<String>::new(),
            "no grant yet: nothing foreign to discover"
        );

        let after = session_doc(
            8,
            100_000,
            json!({
                "primary": personal_mail,
                // The grant that arrived between opens.
                "delegate": {"name": "Delegate", "isPersonal": false, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}},
                // Personal sibling account: never foreign.
                "archive": {"name": "Archive", "isPersonal": true, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}},
                // Non-personal but no mail capability: not a mail share.
                "files": {"name": "Files", "isPersonal": false, "isReadOnly": true, "accountCapabilities": {"urn:ietf:params:jmap:blob": {}}}
            }),
        );
        assert_eq!(
            foreign_mail_account_ids(&after, "primary"),
            vec!["delegate".to_string()],
            "the new grant, and only the new grant, is discovered"
        );
    }

    #[tokio::test]
    async fn open_state_probes_are_one_recorded_request_per_account() {
        let client = scripted_client([open_reply("primary", "p"), open_reply("shared", "s")]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));

        let primary_seed = seed_account_state(&primary).await.expect("primary seed");
        let shared_seed = seed_foreign_account(&shared).await.expect("shared seed");

        assert_eq!(primary_seed.0, "email-p");
        assert_eq!(shared_seed.email_state, "email-s");
        let requests = client.transport().requests();
        assert_eq!(requests.len(), 2);
        assert_open_batch(&requests[0], "primary");
        assert_open_batch(&requests[1], "shared");
    }

    #[tokio::test]
    async fn armed_open_script_fails_at_the_unexpected_request() {
        let client = scripted_client([open_reply("primary", "p")]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        seed_account_state(&primary)
            .await
            .expect("first scripted request");
        let error = seed_account_state(&primary)
            .await
            .expect_err("second unscripted request must fail");

        assert!(
            error
                .to_string()
                .contains("scripted JMAP transport exhausted at API request #2")
        );
        assert_eq!(client.transport().requests().len(), 2);
    }

    /// `maxCallsInRequest` is a hard limit. RFC 8620 §2 recommends
    /// servers allow at least 16, but a client that treats the
    /// recommendation as a floor hands a conservative server a batch it
    /// rejects wholesale - which fails a primary open and silently drops
    /// every foreign account.
    #[tokio::test]
    async fn a_session_that_forbids_two_calls_gets_one_request_per_probe() {
        let client = scripted_client_with_session(
            session_with_limits(1, 100_000),
            serial_open_replies("primary", "p"),
        );
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let seed = seed_account_state(&primary).await.expect("serial seed");

        assert_eq!(seed.0, "email-p");
        assert_eq!(seed.1, "mailbox-p");
        let requests = client.transport().requests();
        assert_eq!(requests.len(), 2, "one request per probe");
        for request in &requests {
            assert_eq!(request["methodCalls"].as_array().map(Vec::len), Some(1));
        }
        assert_eq!(requests[0]["methodCalls"][0][0], "Email/get");
        assert_eq!(requests[1]["methodCalls"][0][0], "Mailbox/get");
    }

    /// `maxSizeRequest` is the other hard limit, and it can fall between
    /// the individual probes and their batch - so the call-count check
    /// alone is not enough.
    #[tokio::test]
    async fn a_size_limit_between_one_probe_and_the_batch_gets_one_request_per_probe() {
        // Big enough for either single probe, too small for both.
        let one_probe = {
            let client = scripted_client([open_reply("primary", "p")]);
            let primary = client
                .primary_account::<capability::Mail>()
                .expect("primary mail account");
            let mut request = primary.build();
            let (email, _) = open_probe_methods();
            request.call(email).expect("probe encodes");
            request.encoded_len().expect("probe encodes")
        };
        let client = scripted_client_with_session(
            session_with_limits(16, one_probe),
            serial_open_replies("primary", "p"),
        );
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let seed = seed_account_state(&primary).await.expect("serial seed");

        assert_eq!(seed.0, "email-p");
        let requests = client.transport().requests();
        assert_eq!(requests.len(), 2, "one request per probe");
    }

    /// A 429 never reaches a caller as `bifrost_net::Error::Status`:
    /// bifrost-net retries it and surfaces `RateLimited` with the final
    /// response preserved. The JMAP boundary has to lift the problem
    /// document back out of that evidence, or the provider's own
    /// explanation of the throttle is lost.
    #[tokio::test]
    async fn a_429_arrives_as_rate_limited_with_its_problem_document() {
        let client = scripted_client([ScriptedReply::status(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            Bytes::from_static(
                br#"{"type":"urn:ietf:params:jmap:error:limit","limit":"concurrentRequests"}"#,
            ),
        )
        .header("retry-after", "120")]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let error = seed_account_state(&primary)
            .await
            .expect_err("429 is a typed error, never a JMAP response");

        let crate::Error::Problem {
            transport: Some(transport),
            ..
        } = error
        else {
            panic!("a 429 carrying a problem document is a Problem error");
        };
        assert!(
            matches!(
                transport.net,
                Some(bifrost_net::Error::RateLimited {
                    retry_after: Some(_),
                    ..
                })
            ),
            "production never produces Error::Status for a 429"
        );
    }

    /// A retryable 5xx is `RetryBudgetExhausted`, not `Status`, for the
    /// same reason.
    #[tokio::test]
    async fn a_5xx_arrives_as_retry_budget_exhausted() {
        let client = scripted_client([ScriptedReply::status(
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            Bytes::from_static(br#"{"type":"about:blank","status":503}"#),
        )]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let error = seed_account_state(&primary)
            .await
            .expect_err("503 is a typed error, never a JMAP response");

        let crate::Error::Problem {
            transport: Some(transport),
            ..
        } = error
        else {
            panic!("a 503 carrying a problem document is a Problem error");
        };
        assert!(
            matches!(
                transport.net,
                Some(bifrost_net::Error::RetryBudgetExhausted { .. })
            ),
            "production never produces Error::Status for a 5xx"
        );
    }

    /// A status the retry policy does not cover is the one shape that
    /// really is `Error::Status`.
    #[tokio::test]
    async fn a_non_retryable_4xx_arrives_as_a_status_error() {
        let client = scripted_client([ScriptedReply::status(
            reqwest::StatusCode::BAD_REQUEST,
            Bytes::from_static(br#"{"type":"urn:ietf:params:jmap:error:notRequest"}"#),
        )]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let error = seed_account_state(&primary)
            .await
            .expect_err("400 is a typed error, never a JMAP response");

        let crate::Error::Problem {
            transport: Some(transport),
            ..
        } = error
        else {
            panic!("a 400 carrying a problem document is a Problem error");
        };
        assert!(matches!(
            transport.net,
            Some(bifrost_net::Error::Status {
                code: reqwest::StatusCode::BAD_REQUEST,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn scripted_typed_error_reaches_the_sync_request_unchanged() {
        let client = scripted_client([ScriptedReply::Error(TransportError::new(
            "scripted connection reset",
        ))]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let error = seed_account_state(&primary)
            .await
            .expect_err("scripted transport error");
        assert!(matches!(error, crate::Error::Transport(_)));
    }

    #[tokio::test]
    async fn a_revoked_foreign_share_becomes_a_terminal_named_skip() {
        let client = scripted_client([method_error("shared", "forbidden")]);
        let shared = JmapMailAccount::new(client, JmapAccountId::new("shared"));

        let Err(skip) = seed_foreign_account_or_skip("shared", &shared).await else {
            panic!("a forbidden foreign probe must become a skip, not a seed");
        };

        assert!(
            matches!(&skip.scope, bifrost_types::ErrorScope::Mailbox { id } if id == "shared"),
            "the skip names the degraded foreign account: {:?}",
            skip.scope
        );
        assert!(
            matches!(
                skip.error.recovery(),
                bifrost_types::RecoveryClass::NoPermission { .. }
            ),
            "a revoked grant classifies terminal NoPermission: {:?}",
            skip.error.recovery()
        );
    }

    #[tokio::test]
    async fn a_retry_exhausted_foreign_probe_becomes_a_retryable_skip() {
        let client = scripted_client([ScriptedReply::status(
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            Bytes::from_static(br#"{"type":"about:blank","status":503}"#),
        )]);
        let shared = JmapMailAccount::new(client, JmapAccountId::new("shared"));

        let Err(skip) = seed_foreign_account_or_skip("shared", &shared).await else {
            panic!("a transient foreign probe must not erase the share silently");
        };

        assert!(
            matches!(&skip.scope, bifrost_types::ErrorScope::Mailbox { id } if id == "shared"),
            "the skip names the degraded foreign account: {:?}",
            skip.scope
        );
        assert!(
            skip.error.recovery().is_retryable(),
            "retry exhaustion from bifrost-net stays retryable on the skip \
             lane, so a consumer knows a reopen can heal it"
        );
    }

    #[test]
    fn configured_email_uses_basic_email_username_only() {
        let basic = JmapCredentials::Basic {
            username: "Ada@Example.Test".to_string(),
            password: "secret".to_string(),
        };
        assert_eq!(basic.configured_email(), Some("Ada@Example.Test"));

        let non_email = JmapCredentials::Basic {
            username: "ada".to_string(),
            password: "secret".to_string(),
        };
        assert_eq!(non_email.configured_email(), None);

        let bearer = JmapCredentials::bearer("token");
        assert_eq!(bearer.configured_email(), None);
    }

    #[tokio::test]
    async fn bearer_source_threads_token_source() {
        use crate::client::Authorization;
        use bifrost_net::AccessToken;

        let source = StaticTokenSource::new("old-token", None);
        let creds = JmapCredentials::bearer_source(Arc::new(source.clone()));
        // The shared source threads through to the client credential and
        // the account-net token source unchanged.
        let client_creds = creds.into_client_credentials();
        let authorization = Authorization::from_credentials_for_test(client_creds);
        let threaded = authorization.account_token_source();
        assert_eq!(
            threaded
                .current()
                .await
                .expect("static source infallible")
                .as_str(),
            "old-token"
        );
        // A token rotated on the original source is visible through the
        // threaded source with no reopen.
        source.set(AccessToken::new("new-token", None));
        assert_eq!(
            threaded
                .current()
                .await
                .expect("static source infallible")
                .as_str(),
            "new-token"
        );
    }

    #[test]
    fn push_email_alias_normalizes_and_deduplicates() {
        let mut emails = Vec::new();

        push_email_alias(&mut emails, "Ada@Example.Test");
        push_email_alias(&mut emails, "ada@example.test");
        push_email_alias(&mut emails, "not-an-email");

        assert_eq!(emails, vec!["ada@example.test"]);
    }
}
