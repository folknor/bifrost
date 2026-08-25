use super::scope::AccountOperation;
use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AccountErrorKind {
    Transport(TransportErrorKind),
    Authentication(AuthErrorKind),
    Authorization(AccessErrorKind),
    Server(ServerErrorKind),
    SyncState(SyncStateErrorKind),
    ConcurrencyConflict,
    Request(RequestErrorKind),
    NotFound(ResourceKind),
    Unsupported(AccountOperation),
    Protocol(ProtocolErrorKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum TransportErrorKind {
    Network,
    Timeout,
    Tls,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum RequestErrorKind {
    Malformed,
    BatchInputInvalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum AuthErrorKind {
    Expired,
    RefreshTransient,
    Revoked,
    ReauthorizationRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum AccessErrorKind {
    AdminConsentRequired,
    ConditionalAccessBlocked,
    PolicyBlocked,
    InsufficientScope,
    PermissionDenied,
    AccountDisabled,
    MailboxUnavailable { kind: MailboxUnavailableKind },
    MailboxNotLicensed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum ServerErrorKind {
    Unavailable,
    RateLimited,
    QuotaExhausted,
    Error { status: Option<u16> },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum SyncStateErrorKind {
    CursorInvalid,
    StrategyFailure,
    ScopeCapabilityLost,
    SchemaIncompatible,
    CapabilityChanged,
    OperatorOverrideNeeded,
    /// Access to one cursor scope was revoked mid-sync (an admin pulled
    /// rights on a single shared/other-user IMAP folder). Quarantine just
    /// that scope; do NOT escalate to account-wide auth loss.
    ScopeRevoked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum ProtocolErrorKind {
    ParseFailed,
    MissingField,
    ContractViolation,
    PartialResponse,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
#[non_exhaustive]
pub enum ResourceKind {
    Message,
    Mailbox,
    Thread,
    Calendar,
    Contact,
    Draft,
    Identity,
    Vacation,
    /// Provider-neutral push subscription. Covers Gmail Pub/Sub watch,
    /// Graph webhook subscriptions, JMAP push subscriptions, and any
    /// future IMAP IDLE abstraction.
    PushSubscription,
    /// Account-level resource (Gmail `GmailResource::Account`, Graph
    /// service principals, etc.). Distinct from `Message` so consumers
    /// route an account-level `NotFound` away from the message UX.
    Account,
    /// Server-side mail filter / rule. Covers a ManageSieve script
    /// (RFC 5804 `NONEXISTENT`), a Gmail filter, and a JMAP SieveScript.
    /// The operation vocabulary already treats filters as first class
    /// (`FiltersList` / `FilterCreate` / `FilterUpdate` / `FilterDelete`
    /// / `FilterValidate`), so a missing one has its own resource rather
    /// than borrowing an unrelated variant.
    Filter,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum MailboxUnavailableKind {
    Transient,
    Permanent,
}
