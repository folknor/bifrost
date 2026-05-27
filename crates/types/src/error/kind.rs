use super::scope::AccountOperation;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum MailboxUnavailableKind {
    Transient,
    Permanent,
}
