//! Bifrost IMAP client library.
//!
//! An `IMAP4rev1` (RFC 3501) and `IMAP4rev2` (RFC 9051) async client
//! built on tokio and native-tls. Single crate  -  parser, types, and connection
//! in one place.
//!
//! # Architecture
//!
//! ## Driver task
//!
//! [`ImapConnection`] is a lightweight handle  -  it holds an
//! `mpsc::Sender<DriverCommand>` and a `watch::Receiver` for state
//! snapshots. A dedicated tokio task (the *driver*) owns the TCP/TLS
//! stream exclusively. All public methods take `&self`, submit commands
//! over the channel, and await a result via a `oneshot`. Dropping a
//! future mid-flight cannot corrupt the stream because no caller-side
//! future has access to it  -  the driver completes the in-flight command
//! and only the result is abandoned. This makes `tokio::select!` and
//! `tokio::time::timeout` safe to use around any operation.
//!
//! ## Consumer trait
//!
//! Each IMAP command is executed by a [`Consumer`](connection::dispatch)
//! implementation. The driver feeds the consumer pre-classified responses
//! via `on_response`, then calls `finalize` when the tagged response
//! arrives. Consumers never make routing decisions  -  they only accumulate
//! data they are given. Commands that expect `+` continuations (e.g.
//! AUTHENTICATE) use the separate `ContinuationConsumer` trait with an
//! additional `on_continuation` method.
//!
//! ## Classification truth table
//!
//! The function [`classify`](codec::classification::classify) is the
//! single source of truth for whether an untagged response belongs to
//! the current command's result or to the asynchronous event queue.
//! Every row cites the RFC section that defines the routing rule.
//! The dispatcher calls `classify` before each response and routes
//! mechanically  -  consumers have no routing decision to make.
//!
//! ## Typed event queue
//!
//! Asynchronous server notifications  -  ALERTs, EXISTS/EXPUNGE changes,
//! NOTIFY data, BYE  -  arrive as [`TypedEvent`]s. Poll them with
//! [`drain_events`](ImapConnection::drain_events) (non-blocking) or
//! [`next_event`](ImapConnection::next_event) (with timeout). The
//! driver publishes events via a non-blocking `DriverEventSink` that
//! can never suspend the driver's select loop.
//!
//! ## Wire reader and protocol state
//!
//! All wire reads flow through a private `WireReader` in `mod wire`,
//! which is visible only within the `connection` module. All protocol
//! state mutations flow through
//! `ProtocolState::apply_side_effects` in `mod state`  -  the primary
//! mutator. The state module's fields are `pub(self)`, so direct
//! field assignment from outside `mod state` is a compile error.
//!
//! ## `MailboxName`
//!
//! Every mailbox name in every public type is [`MailboxName`]  -  a
//! validated, decoded UTF-8 newtype with no `From<String>` impl. The
//! only constructors are `new` (public, validating) and `from_decoded`
//! (`pub(crate)`, for already-parsed wire data in the codec). The
//! compiler refuses to smuggle wire-form bytes through any public type.

// pub: consumers register the IMAP AccountFactory through this module.
pub mod account;
pub mod error;
pub mod types;

mod codec;
mod connection;

/// Re-export the small address type used by IMAP envelope conversion helpers.
pub use crate::types::Address;
pub use connection::{
    IdleEvent, ImapConfig, ImapConnection, SearchResult, SessionState, TcpKeepalive, TlsMode,
    typed_event::TypedEvent,
};
pub use error::{
    AuthMechanismRejection, AuthMechanismRejectionReason, AuthPolicyFailure, Error, ErrorCategory,
    Recovery,
};
pub use types::{
    AclEntry, AppendLimitPolicy, AppendMessage, AuthMechanism, AuthOutcome, AuthPolicy,
    BinarySection, BodySection, BodyStructure, Capability, ContentDisposition, ContinuationRequest,
    CopyResult, Credentials, Envelope, EnvelopeAddress, EsearchResponse, EventImpact,
    ExpungeResult, FetchAttr, FetchResponse, Flag, GmailMessageId, GmailThreadId, GreetingResponse,
    GreetingStatus, ImapAtom, IntoSecretString, ListRightsResponse, MailboxAttribute,
    MailboxFilter, MailboxInfo, MailboxName, MetadataEntry, MetadataResult, ModSeq, MoveResult,
    NamespaceDescriptor, NamespaceResponse, NotifyEvent, NotifyEventGroup, NotifySetParams,
    ObjectId, QresyncParams, QuotaResource, QuotaRootResponse, Response, ResponseCode,
    SearchCriteria, SecretString, SelectOptions, SelectedMailbox, Seq, SeqSet, SequenceSet,
    ServerProfile, SpecialUse, StatusItem, StatusKind, StatusResult, StoreOperation, StoreResult,
    SyncFetchRequest, SyncFetchResult, SyncSelectOptions, SyncSelectResult, TaggedResponse,
    ThreadNode, Uid, UidRange, UidSet, UidValidity, UntaggedResponse, UntaggedStatus,
    ValidationError,
};

/// Result type alias for IMAP operations.
pub type Result<T> = std::result::Result<T, Error>;
